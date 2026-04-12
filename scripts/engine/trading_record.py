#!/usr/bin/env python3
"""
engine/trading_record.py — 交易记录与 P&L 追踪

职责：
  - record_buy / record_sell：记录买卖，自动算持仓天数和盈亏
  - get_trade_history：读取历史记录
  - calc_stats：统计胜率/盈亏比/最大亏损等
  - check_weekly_buy_count：本周已买次数（风控用）

数据存储：data/交易记录/YYYY-MM.csv（每月一个文件）
格式：股票代码,名称,日期,操作,价格,数量,金额,盈亏,盈亏率,持有天数,卖出原因,记录时间,佣金,印花税,总费用,市场

费率规则（按用户实际券商）：
  - A股/ETF：佣金 0.12‰，最低 5 元/笔；卖出加收 0.1% 印花税
  - 港股：佣金 0.12‰，最低 5 元/笔；无印花税
"""

import os
import sys
import csv
import importlib
import warnings
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, _PROJECT_ROOT)

os.environ["TQDM_DISABLE"] = "1"
warnings.filterwarnings("ignore")

# 持仓缓存（运行时）：解决同一股票多次买卖的问题
# key: stock_code, value: {"shares": int, "cost": float, "buy_date": str, "buy_price": float}
_HOLDINGS: dict = {}

TRADE_RECORD_DIR = Path(_PROJECT_ROOT) / "data" / "交易记录"
TRADE_RECORD_DIR.mkdir(parents=True, exist_ok=True)
DEFAULT_ACTIVITY_SCOPE = "cn_a_system"
_ACTIVITY_STATE_MODULES = (
    "scripts.state",
    "scripts.state.service",
    "scripts.utils.runtime_state",
    "scripts.engine.trade_state",
    "scripts.engine.activity_state",
    "scripts.engine.state",
    "scripts.engine.ledger_state",
    "scripts.pipeline.state",
)


def _get_record_path(dt: Optional[date] = None) -> Path:
    if dt is None:
        dt = date.today()
    return TRADE_RECORD_DIR / f"{dt.strftime('%Y-%m')}.csv"


def _detect_market(code: str) -> str:
    """
    根据股票代码判断市场类型。

    Returns:
        "HK"  — 港股（如 09927、009927、99927，或含 .HK / HK 后缀）
        "ETF" — 基金/ETF（以 5 开头，如 512000）
        "A"   — A股（其他所有）
    """
    c = str(code).strip().upper()
    # 含 HK 标记的视为港股
    if c.endswith("HK") or ".HK" in c or "_HK" in c:
        return "HK"
    # 纯数字码：5 开头 = ETF，其余都走 A 股规则
    # 港股代码通常 5 位且首位为 9（09927），或首位为 0 但含 HK 标记
    c_stripped = c.lstrip("0")
    if c_stripped.startswith("9"):
        return "HK"
    if c.startswith("5"):
        return "ETF"
    return "A"


def _calc_fees(amount: float, market: str, side: str) -> tuple[float, float, float]:
    """
    计算交易费用。

    Args:
        amount: 成交金额（价格 × 股数）
        market: 市场类型（"A" / "ETF" / "HK"）
        side:  "BUY" / "SELL"

    Returns:
        (commission, stamp_duty, total_fee)
        commission: 佣金，按 0.12‰ 计，最低 5 元
        stamp_duty: 印花税（仅 A股/ETF 卖出收取 0.1%）
        total_fee: 佣金 + 印花税
    """
    commission_rate = 0.00012  # 0.12‰
    commission = round(amount * commission_rate, 2)
    if commission < 5.0:
        commission = 5.0

    stamp_duty = 0.0
    if side == "SELL" and market in ("A", "ETF"):
        stamp_duty = round(amount * 0.001, 2)  # 0.1%

    total_fee = round(commission + stamp_duty, 2)
    return commission, stamp_duty, total_fee


def _ensure_header(path: Path):
    if not path.exists():
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["股票代码", "名称", "日期", "操作",
                             "价格", "数量", "金额", "盈亏", "盈亏率",
                             "持有天数", "卖出原因", "记录时间",
                             "佣金", "印花税", "总费用", "市场"])


def _holdings_key(code: str) -> str:
    return code.strip()


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value in [None, ""]:
            return default
        if isinstance(value, str):
            value = value.replace("¥", "").replace(",", "").replace("%", "").strip()
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value, default: int = 0) -> int:
    try:
        if value in [None, ""]:
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _normalize_trade_event(event: dict, scope: str = DEFAULT_ACTIVITY_SCOPE) -> dict:
    """把不同来源的交易事件统一成周报可消费的结构。"""
    if not isinstance(event, dict):
        event = {}

    raw_action = str(
        event.get("action")
        or event.get("side")
        or event.get("operation")
        or event.get("操作")
        or ""
    ).strip()
    action_map = {
        "买入": "BUY",
        "卖出": "SELL",
        "buy": "BUY",
        "sell": "SELL",
        "BUY": "BUY",
        "SELL": "SELL",
    }
    action = action_map.get(raw_action, raw_action.upper() if raw_action else "UNKNOWN")

    trade_date = str(
        event.get("trade_date")
        or event.get("date")
        or event.get("日期")
        or ""
    ).strip()
    if len(trade_date) > 10:
        trade_date = trade_date[:10]

    timestamp = str(
        event.get("timestamp")
        or event.get("trade_time")
        or event.get("记录时间")
        or event.get("datetime")
        or ""
    ).strip()

    return {
        "scope": str(event.get("scope") or scope or DEFAULT_ACTIVITY_SCOPE),
        "trade_date": trade_date,
        "timestamp": timestamp,
        "action": action,
        "code": str(
            event.get("code")
            or event.get("stock_code")
            or event.get("股票代码")
            or event.get("secuCode")
            or ""
        ).strip(),
        "name": str(
            event.get("name")
            or event.get("stock_name")
            or event.get("股票")
            or event.get("名称")
            or event.get("stockName")
            or ""
        ).strip(),
        "shares": _safe_int(
            event.get("shares")
            or event.get("qty")
            or event.get("volume")
            or event.get("数量")
            or event.get("currentQty")
            or 0
        ),
        "price": round(_safe_float(
            event.get("price")
            or event.get("trade_price")
            or event.get("成交价")
            or event.get("价格")
            or 0
        ), 3),
        "amount": round(_safe_float(
            event.get("amount")
            or event.get("成交额")
            or event.get("金额")
            or 0
        ), 2),
        "realized_pnl": round(_safe_float(
            event.get("realized_pnl")
            or event.get("pnl")
            or event.get("盈亏")
            or 0
        ), 2),
        "reason": str(
            event.get("reason")
            or event.get("卖出原因")
            or event.get("备注")
            or ""
        ).strip(),
        "reason_code": str(
            event.get("reason_code")
            or event.get("reasonCode")
            or ""
        ).strip(),
        "source": str(event.get("source") or "structured_state"),
    }


def _event_sort_key(event: dict):
    trade_date = str(event.get("trade_date", "")).strip()
    timestamp = str(event.get("timestamp", "")).strip()
    return (trade_date, timestamp)


def _load_external_activity_summary(window: int, scope: str) -> Optional[dict]:
    """优先尝试接入新的统一状态接口。"""
    for module_name in _ACTIVITY_STATE_MODULES:
        try:
            module = importlib.import_module(module_name)
        except Exception:
            continue
        loader = getattr(module, "load_activity_summary", None)
        if callable(loader):
            try:
                return loader(window, scope=scope)
            except Exception:
                continue
    return None


def _load_activity_summary_from_records(records: list, window: int,
                                       scope: str) -> dict:
    """从本地 CSV 交易记录回退生成活动摘要。"""
    cutoff = date.today() - timedelta(days=max(window - 1, 0))
    trade_events = []
    for row in records:
        trade_date = str(row.get("日期", "")).strip()
        if not trade_date:
            continue
        try:
            row_date = datetime.strptime(trade_date[:10], "%Y-%m-%d").date()
        except Exception:
            continue
        if row_date < cutoff:
            continue

        action = str(row.get("操作", "")).strip().upper()
        pnl = _safe_float(row.get("盈亏", 0), 0.0)
        trade_events.append(_normalize_trade_event({
            "scope": scope,
            "trade_date": trade_date[:10],
            "timestamp": row.get("记录时间", ""),
            "action": action,
            "code": row.get("股票代码", ""),
            "name": row.get("名称", ""),
            "shares": row.get("数量", 0),
            "price": row.get("价格", 0),
            "amount": row.get("金额", 0),
            "realized_pnl": pnl if action == "SELL" else 0.0,
            "reason": row.get("卖出原因", ""),
            "source": "csv_fallback",
        }, scope=scope))

    buy_count = sum(1 for event in trade_events if event["action"] == "BUY")
    sell_count = sum(1 for event in trade_events if event["action"] == "SELL")
    realized_pnl = round(
        sum(event["realized_pnl"] for event in trade_events if event["action"] == "SELL"),
        2,
    )
    trade_events.sort(key=_event_sort_key)

    return {
        "scope": scope,
        "window": window,
        "weekly_buy_count": buy_count,
        "weekly_sell_count": sell_count,
        "buy_count": buy_count,
        "sell_count": sell_count,
        "trade_count": len(trade_events),
        "realized_pnl": realized_pnl,
        "trade_events": trade_events,
        "source": "csv_fallback",
    }


def load_activity_summary(window: int, scope: str = DEFAULT_ACTIVITY_SCOPE) -> dict:
    """
    读取结构化交易活动摘要。

    优先使用新的统一状态接口；如果尚未接通，则回退到本地 CSV 记录。
    """
    external = _load_external_activity_summary(window, scope)
    if external:
        raw_trade_events = external.get("trade_events", []) or []
        trade_events = [
            _normalize_trade_event(event, scope=scope)
            for event in raw_trade_events
        ]

        buy_count = _safe_int(
            external.get("weekly_buy_count")
            or external.get("buy_count")
            or sum(1 for event in trade_events if event["action"] == "BUY")
        )
        sell_count = _safe_int(
            external.get("weekly_sell_count")
            or external.get("sell_count")
            or sum(1 for event in trade_events if event["action"] == "SELL")
        )
        realized_pnl = round(
            _safe_float(external.get("realized_pnl", 0.0)),
            2,
        )
        if realized_pnl == 0.0 and trade_events:
            realized_pnl = round(
                sum(
                    event["realized_pnl"]
                    for event in trade_events
                    if event["action"] == "SELL"
                ),
                2,
            )
        trade_events.sort(key=_event_sort_key)

        return {
            "scope": scope,
            "window": window,
            "weekly_buy_count": buy_count,
            "weekly_sell_count": sell_count,
            "buy_count": buy_count,
            "sell_count": sell_count,
            "trade_count": _safe_int(external.get("trade_count", len(trade_events))),
            "realized_pnl": realized_pnl,
            "trade_events": trade_events,
            "source": str(external.get("source") or "external_state"),
        }

    return _load_activity_summary_from_records(load_all_records(), window, scope)


def record_buy(code: str, name: str, price: float, shares: int,
               trade_date: Optional[str] = None) -> dict:
    """
    记录买入

    Args:
        code: 股票代码
        name: 股票名称
        price: 买入价格
        shares: 买入股数
        trade_date: 交易日期，默认为今天

    Returns:
        {"success": bool, "message": str}
    """
    if trade_date is None:
        trade_date = date.today().isoformat()

    record_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    amount = round(price * shares, 2)
    market = _detect_market(code)
    commission, stamp_duty, total_fee = _calc_fees(amount, market, "BUY")
    total_cost = round(amount + total_fee, 2)

    _ensure_header(_get_record_path())
    path = _get_record_path()

    # 写入 CSV（买时不算盈亏，持有天数从买时算起）
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            code, name, trade_date, "BUY",
            f"{price:.3f}", shares, f"{total_cost:.2f}",
            "",  # 盈亏空白，买入时未知
            "",  # 盈亏率空白
            "",  # 持有天数空白
            "",  # 卖出原因空白
            record_time,
            f"{commission:.2f}",
            f"{stamp_duty:.2f}",
            f"{total_fee:.2f}",
            market,
        ])

    # 更新运行时持仓缓存
    key = _holdings_key(code)
    if key in _HOLDINGS:
        old = _HOLDINGS[key]
        total_shares = old["shares"] + shares
        avg_price = round((old["cost"] * old["shares"] + price * shares) / total_shares, 4)
        _HOLDINGS[key] = {
            "shares": total_shares,
            "cost": avg_price,
            "buy_date": old["buy_date"],  # 保留最早买入日
            "buy_price": avg_price,
        }
    else:
        _HOLDINGS[key] = {
            "shares": shares,
            "cost": price,
            "buy_date": trade_date,
            "buy_price": price,
        }

    return {
        "success": True,
        "message": f"记录买入 {name}({code}) {shares}股@{price}，含佣金{commission:.2f}元，持仓总成本约{total_cost:.0f}",
        "total_cost": total_cost,
        "commission": commission,
        "stamp_duty": stamp_duty,
        "total_fee": total_fee,
        "market": market,
    }


def record_sell(code: str, name: str, price: float, shares: int,
                reason: str = "",
                trade_date: Optional[str] = None) -> dict:
    """
    记录卖出

    Args:
        code: 股票代码
        name: 股票名称
        price: 卖出价格
        shares: 卖出股数
        reason: 卖出原因（止盈1/止盈2/止损/时间止损/清仓/其他）
        trade_date: 交易日期，默认为今天

    Returns:
        {"success": bool, "pnl": float, "pnl_pct": float, "message": str}
    """
    if trade_date is None:
        trade_date = date.today().isoformat()

    record_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    key = _holdings_key(code)
    holding = _HOLDINGS.get(key)

    if not holding:
        return {"success": False, "pnl": 0, "pnl_pct": 0,
                "message": f"错误：没有找到 {code} 的持仓记录"}

    # 计算盈亏（用实际费率扣费）
    buy_cost = holding["cost"]
    sell_amount = round(price * shares, 2)
    market = _detect_market(code)
    commission, stamp_duty, total_fee = _calc_fees(sell_amount, market, "SELL")
    net_proceed = round(sell_amount - total_fee, 2)
    cost_basis = round(buy_cost * shares, 2)
    pnl = round(net_proceed - cost_basis, 2)
    pnl_pct = round(pnl / cost_basis * 100, 2) if cost_basis else 0

    # 持有天数
    buy_date_str = holding["buy_date"]
    try:
        buy_dt = datetime.strptime(buy_date_str, "%Y-%m-%d").date()
        trade_dt = datetime.strptime(trade_date, "%Y-%m-%d").date()
        hold_days = (trade_dt - buy_dt).days
    except Exception:
        hold_days = 0

    _ensure_header(_get_record_path())
    path = _get_record_path()

    # 追加卖出记录
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            code, name, trade_date, "SELL",
            f"{price:.3f}", shares, f"{sell_amount:.2f}",
            f"{pnl:.2f}", f"{pnl_pct}%",
            hold_days,
            reason,
            record_time,
            f"{commission:.2f}",
            f"{stamp_duty:.2f}",
            f"{total_fee:.2f}",
            market,
        ])

    # 更新持仓缓存
    remaining = holding["shares"] - shares
    if remaining <= 0:
        _HOLDINGS.pop(key, None)
    else:
        _HOLDINGS[key]["shares"] = remaining

    msg = (f"记录卖出 {name}({code}) {shares}股@{price}，"
           f"盈亏{'+' if pnl >= 0 else ''}{pnl:.2f}元({pnl_pct}%)，"
           f"含佣金{commission:.2f}+印花税{stamp_duty:.2f}，"
           f"持有{hold_days}天，原因:{reason}")
    return {"success": True, "pnl": pnl, "pnl_pct": pnl_pct,
            "hold_days": hold_days, "message": msg,
            "commission": commission, "stamp_duty": stamp_duty, "total_fee": total_fee,
            "net_proceed": net_proceed, "market": market}


def load_all_records() -> list:
    """加载所有交易记录（按日期倒序）"""
    records = []
    if not TRADE_RECORD_DIR.exists():
        return records
    for csv_file in sorted(TRADE_RECORD_DIR.glob("*.csv"), reverse=True):
        with open(csv_file, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                records.append(row)
    return records


def calc_stats(records: Optional[list] = None) -> dict:
    """
    计算交易统计

    Returns:
        {
            total_trades, win_count, loss_count, win_rate,
            avg_win, avg_loss, profit_ratio,
            total_pnl, max_win, max_loss,
            avg_hold_days, total_invested
        }
    """
    if records is None:
        records = load_all_records()

    sells = [r for r in records if r.get("操作") == "SELL" and r.get("盈亏")]
    if not sells:
        return {"total_trades": 0, "win_count": 0, "loss_count": 0,
                "win_rate": 0, "total_pnl": 0}

    pnls = []
    hold_days_list = []
    total_invested = 0

    for r in sells:
        try:
            pnl = float(r["盈亏"])
            hold = int(r.get("持有天数", 0) or 0)
            invested = float(r["金额"])
            pnls.append(pnl)
            hold_days_list.append(hold)
            total_invested += invested
        except (ValueError, KeyError):
            continue

    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]

    total_pnl = sum(pnls)
    return {
        "total_trades": len(sells),
        "win_count": len(wins),
        "loss_count": len(losses),
        "win_rate": round(len(wins) / len(sells) * 100, 1) if sells else 0,
        "avg_win": round(sum(wins) / len(wins), 2) if wins else 0,
        "avg_loss": round(sum(losses) / len(losses), 2) if losses else 0,
        "profit_ratio": (abs(sum(wins)) / abs(sum(losses)))
                        if losses and sum(losses) != 0 else 0,
        "total_pnl": round(total_pnl, 2),
        "max_win": max(wins) if wins else 0,
        "max_loss": min(losses) if losses else 0,
        "avg_hold_days": round(sum(hold_days_list) / len(hold_days_list), 1)
                        if hold_days_list else 0,
        "total_invested": round(total_invested, 2),
    }


def get_weekly_buy_count(records: Optional[list] = None) -> int:
    """本周买入次数（用于风控：每周最多2笔）"""
    if records is not None:
        today = date.today()
        week_start = today.isocalendar()[1]
        year = today.year
        total = 0
        for r in records:
            if r.get("操作") != "BUY":
                continue
            trade_date = str(r.get("日期", "")).strip()
            if len(trade_date) < 10:
                continue
            try:
                trade_dt = datetime.strptime(trade_date[:10], "%Y-%m-%d")
            except Exception:
                continue
            if trade_dt.year == year and trade_dt.isocalendar()[1] == week_start:
                total += 1
        return total
    summary = load_activity_summary(7, scope=DEFAULT_ACTIVITY_SCOPE)
    return _safe_int(summary.get("weekly_buy_count", summary.get("buy_count", 0)))


def get_open_positions() -> dict:
    """返回当前运行时持仓"""
    return dict(_HOLDINGS)


def format_trade_table(records: Optional[list] = None,
                       limit: int = 20) -> str:
    """格式化交易记录表格（Markdown）"""
    if records is None:
        records = load_all_records()
    sells = [r for r in records if r.get("操作") == "SELL" and r.get("盈亏")]
    recent = sells[-limit:] if len(sells) > limit else sells

    lines = [
        "| 日期 | 股票 | 代码 | 操作 | 价格 | 股数 | 盈亏 | 盈亏率 | 持有天数 | 原因 |",
        "|------|------|------|------|------|------|------|--------|----------|------|",
    ]
    for r in recent:
        code = r.get("股票代码", "")
        name = r.get("名称", "")
        date_str = r.get("日期", "")[5:]  # MM-DD
        action = r.get("操作", "")
        price = r.get("价格", "")
        shares = r.get("数量", "")
        pnl = r.get("盈亏", "")
        pnl_pct = r.get("盈亏率", "")
        days = r.get("持有天数", "")
        reason = r.get("卖出原因", "")
        lines.append(
            f"| {date_str} | {name} | {code} | {action} | "
            f"{price} | {shares} | {pnl} | {pnl_pct} | {days}天 | {reason} |"
        )
    return "\n".join(lines)


def format_stats_summary(stats: dict) -> str:
    """格式化统计摘要"""
    if stats["total_trades"] == 0:
        return "暂无交易记录"
    return (
        f"**总计 {stats['total_trades']} 笔 | "
        f"胜率 {stats['win_rate']}% | "
        f"盈亏比 {stats['profit_ratio']:.2f}**\n"
        f"- 总收益: {'+' if stats['total_pnl'] >= 0 else ''}{stats['total_pnl']:.0f}元\n"
        f"- 均胜: +{stats['avg_win']:.0f}元 | 均亏: {stats['avg_loss']:.0f}元\n"
        f"- 最大单笔盈利: +{stats['max_win']:.0f}元 | 最大亏损: {stats['max_loss']:.0f}元\n"
        f"- 平均持仓: {stats['avg_hold_days']:.1f}天"
    )


if __name__ == "__main__":
    import json
    records = load_all_records()
    stats = calc_stats(records)
    print("=== 交易统计 ===")
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    print()
    print("=== 最近交易 ===")
    print(format_trade_table(records, limit=10))
    print()
    print("=== 本周买入次数 ===")
    print(get_weekly_buy_count(records))
