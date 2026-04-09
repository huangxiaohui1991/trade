#!/usr/bin/env python3
"""
engine/trading_record.py — 交易记录与 P&L 追踪

职责：
  - record_buy / record_sell：记录买卖，自动算持仓天数和盈亏
  - get_trade_history：读取历史记录
  - calc_stats：统计胜率/盈亏比/最大亏损等
  - check_weekly_buy_count：本周已买次数（风控用）

数据存储：data/交易记录/YYYY-MM.csv（每月一个文件）
格式：股票代码,名称,日期,操作,价格,数量,金额,盈亏,盈亏率,持有天数,卖出原因,记录时间
"""

import os
import sys
import csv
import warnings
from dataclasses import dataclass, field
from datetime import datetime, date
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


def _get_record_path(dt: Optional[date] = None) -> Path:
    if dt is None:
        dt = date.today()
    return TRADE_RECORD_DIR / f"{dt.strftime('%Y-%m')}.csv"


def _ensure_header(path: Path):
    if not path.exists():
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["股票代码", "名称", "日期", "操作",
                             "价格", "数量", "金额", "盈亏", "盈亏率",
                             "持有天数", "卖出原因", "记录时间"])


def _holdings_key(code: str) -> str:
    return code.strip()


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
    fee = round(amount * 0.0003, 2)  # 印花税+佣金估算
    total_cost = round(amount + fee, 2)

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
            record_time
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
        "message": f"记录买入 {name}({code}) {shares}股@{price}，持仓价值约{total_cost:.0f}",
        "total_cost": total_cost,
        "fee": fee,
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

    # 计算盈亏
    buy_cost = holding["cost"]
    sell_amount = round(price * shares, 2)
    fee = round(sell_amount * 0.0013, 2)  # 印花税0.1%+佣金约0.023%
    net_proceed = round(sell_amount - fee, 2)
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
            record_time
        ])

    # 更新持仓缓存
    remaining = holding["shares"] - shares
    if remaining <= 0:
        _HOLDINGS.pop(key, None)
    else:
        _HOLDINGS[key]["shares"] = remaining

    msg = (f"记录卖出 {name}({code}) {shares}股@{price}，"
           f"盈亏{'+' if pnl >= 0 else ''}{pnl:.2f}元({pnl_pct}%)，"
           f"持有{hold_days}天，原因:{reason}")
    return {"success": True, "pnl": pnl, "pnl_pct": pnl_pct,
            "hold_days": hold_days, "message": msg}


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
    if records is None:
        records = load_all_records()

    today = date.today()
    week_start = today.isocalendar()[1]
    year = today.year

    buys = [r for r in records
            if r.get("操作") == "BUY"
            and r.get("日期", "")[:4] == str(year)
            and datetime.strptime(r["日期"], "%Y-%m-%d").isocalendar()[1] == week_start]
    return len(buys)


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
