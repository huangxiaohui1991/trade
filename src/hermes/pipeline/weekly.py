"""
pipeline/weekly.py — 周报

流程：
1. 统计本周交易（买入/卖出/盈亏）
2. 统计胜率和盈亏比
3. 生成周报 → report_artifacts
4. 写 Obsidian 周复盘
5. 格式化 Discord embed
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from hermes.pipeline.context import PipelineContext

_logger = logging.getLogger(__name__)


def run(ctx: PipelineContext, run_id: str) -> dict:
    """执行周报 pipeline。"""

    # 1. 本周交易统计
    now = datetime.now(timezone.utc)
    week_start = (now - timedelta(days=now.weekday())).strftime("%Y-%m-%d")

    filled_events = ctx.event_store.query(event_type="order.filled")
    closed_events = ctx.event_store.query(event_type="position.closed")

    week_fills = [e for e in filled_events if e.get("occurred_at", "")[:10] >= week_start]
    week_closes = [e for e in closed_events if e.get("occurred_at", "")[:10] >= week_start]

    buy_count = sum(1 for e in week_fills if e["payload"].get("side") == "buy")
    sell_count = sum(1 for e in week_fills if e["payload"].get("side") == "sell")

    # 2. 胜率和盈亏比
    wins = 0
    losses = 0
    total_profit = 0
    total_loss = 0

    for e in week_closes:
        pnl = e["payload"].get("realized_pnl_cents", 0)
        if pnl > 0:
            wins += 1
            total_profit += pnl
        elif pnl < 0:
            losses += 1
            total_loss += abs(pnl)

    total_trades = wins + losses
    win_rate = wins / total_trades if total_trades > 0 else 0
    profit_loss_ratio = (total_profit / total_loss) if total_loss > 0 else float("inf") if total_profit > 0 else 0
    net_pnl_cents = total_profit - total_loss

    # 3. 周报
    week_str = now.strftime("%Y-W%W")
    report = ctx.reporter.generate_weekly_report(week_str)

    # 4. Obsidian 周复盘
    lines = [
        "---",
        f"date: {week_str}",
        "type: weekly_review",
        "tags: [周复盘, 自动更新]",
        "---",
        "",
        f"# 周复盘 — {week_str}",
        "",
        "## 交易统计",
        "",
        f"- 买入: {buy_count} 笔",
        f"- 卖出: {sell_count} 笔",
        f"- 胜率: {win_rate:.0%}（{wins}胜 {losses}负）",
        f"- 盈亏比: {profit_loss_ratio:.2f}",
        f"- 净盈亏: ¥{net_pnl_cents / 100:+,.0f}",
        "",
        "## 当前持仓",
        "",
    ]

    positions = ctx.exec_svc.get_positions()
    if positions:
        lines.append("| 代码 | 名称 | 股数 | 成本 | 风格 |")
        lines.append("|------|------|------|------|------|")
        for p in positions:
            lines.append(f"| {p.code} | {p.name} | {p.shares} | ¥{p.avg_cost:.2f} | {p.style} |")
    else:
        lines.append("空仓")

    content = "\n".join(lines) + "\n"
    ctx.obsidian._write(f"03-分析/周复盘/{week_str}.md", content)

    # 日志追加
    ctx.obsidian.write_daily_log(run_id, f"## 周报生成\n\n{week_str} 周报已生成。{buy_count}买 {sell_count}卖 净盈亏¥{net_pnl_cents/100:+,.0f}")

    _logger.info(f"[weekly] 完成: {buy_count}买 {sell_count}卖 胜率{win_rate:.0%} 净盈亏¥{net_pnl_cents/100:+,.0f}")

    # 6. Discord 推送
    try:
        from hermes.reporting.discord import format_weekly_embed
        from hermes.reporting.discord_sender import send_embed
        embed = format_weekly_embed({
            "week": week_str,
            "buy_count": buy_count, "sell_count": sell_count,
            "win_rate": win_rate, "profit_loss_ratio": profit_loss_ratio,
            "net_pnl_cents": net_pnl_cents,
            "positions": [{"name": p.name, "code": p.code, "shares": p.shares} for p in positions],
        })
        ok, err = send_embed(embed)
        if not ok:
            _logger.warning(f"[weekly] Discord 推送失败: {err}")
    except Exception as e:
        _logger.warning(f"[weekly] Discord 推送异常: {e}")

    return {
        "week": week_str,
        "buy_count": buy_count, "sell_count": sell_count,
        "win_rate": win_rate, "profit_loss_ratio": round(profit_loss_ratio, 2),
        "net_pnl_cents": net_pnl_cents,
    }
