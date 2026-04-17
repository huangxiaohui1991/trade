"""
pipeline/weekly.py — 周报

流程：
1. 统计本周交易（买入/卖出/盈亏）
2. 统计胜率和盈亏比
3. 收集交易明细 + 池子变动
4. 收集模拟盘统计
5. 生成周报 → report_artifacts
6. 写 Obsidian 周复盘（自动数据 + 手动填写区）
7. 格式化 Discord embed
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from hermes.pipeline.context import PipelineContext
from hermes.platform.time import iso_to_local, local_date_bounds_utc, local_now

_logger = logging.getLogger(__name__)


def run(ctx: PipelineContext, run_id: str) -> dict:
    """执行周报 pipeline。"""

    # 1. 本周时间范围
    now = local_now()
    week_start_dt = now - timedelta(days=now.weekday())
    week_end_dt = week_start_dt + timedelta(days=6)
    week_start = week_start_dt.strftime("%Y-%m-%d")
    week_end = week_end_dt.strftime("%Y-%m-%d")
    week_start_display = week_start_dt.strftime("%m/%d")
    week_end_display = week_end_dt.strftime("%m/%d")

    week_start_utc, _ = local_date_bounds_utc(week_start_dt.date())

    # 2. 实盘交易统计
    filled_events = ctx.event_store.query(event_type="order.filled", since=week_start_utc)
    closed_events = ctx.event_store.query(event_type="position.closed", since=week_start_utc)

    week_fills = [e for e in filled_events if iso_to_local(e.get("occurred_at", "")).date() >= week_start_dt.date()]
    week_closes = [e for e in closed_events if iso_to_local(e.get("occurred_at", "")).date() >= week_start_dt.date()]

    buy_count = sum(1 for e in week_fills if e["payload"].get("side") == "buy")
    sell_count = sum(1 for e in week_fills if e["payload"].get("side") == "sell")

    # 3. 胜率和盈亏比
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

    # 4. 交易明细
    trades = []
    for e in week_fills:
        p = e["payload"]
        trades.append({
            "date": iso_to_local(e.get("occurred_at", "")).date().isoformat(),
            "code": p.get("code", ""),
            "name": p.get("name", ""),
            "side": p.get("side", ""),
            "price": p.get("price_cents", 0) / 100,
            "shares": p.get("shares", 0),
            "pnl_cents": 0,
            "note": "",
        })
    # 补充平仓盈亏
    for e in week_closes:
        p = e["payload"]
        code = p.get("code", "")
        pnl = p.get("realized_pnl_cents", 0)
        # 找到对应的卖出记录补充盈亏
        for t in reversed(trades):
            if t["code"] == code and t["side"] == "sell" and t["pnl_cents"] == 0:
                t["pnl_cents"] = pnl
                break

    # 5. 池子变动
    pool_demoted = ctx.event_store.query(event_type="pool.demoted", since=week_start_utc)
    pool_removed = ctx.event_store.query(event_type="pool.removed", since=week_start_utc)
    pool_changes = []
    for e in pool_demoted:
        p = e["payload"]
        pool_changes.append({
            "code": p.get("code", ""), "name": p.get("name", ""),
            "change_type": "demoted", "reason": f"降级: {p.get('reason', '')}",
        })
    for e in pool_removed:
        p = e["payload"]
        pool_changes.append({
            "code": p.get("code", ""), "name": p.get("name", ""),
            "change_type": "removed", "reason": f"移出: 评分 {p.get('score', 0)}",
        })

    # 6. 当前持仓 + 核心池
    positions = ctx.exec_svc.get_positions()
    pos_data = [{"code": p.code, "name": p.name, "shares": p.shares,
                 "avg_cost": p.avg_cost, "style": p.style} for p in positions]

    pool_rows = ctx.conn.execute(
        "SELECT code, name, score FROM projection_candidate_pool "
        "WHERE pool_tier = 'core' ORDER BY score DESC"
    ).fetchall()
    core_pool = [{"code": r["code"], "name": r["name"] or "", "score": r["score"] or 0}
                 for r in pool_rows]

    # 7. 模拟盘统计
    paper_events = ctx.event_store.query(event_type="auto_trade.executed", since=week_start_utc)
    paper_buys = sum(1 for e in paper_events
                     if e.get("payload", {}).get("side") == "buy"
                     and e.get("metadata", {}).get("account") == "paper")
    paper_sells = sum(1 for e in paper_events
                      if e.get("payload", {}).get("side") == "sell"
                      and e.get("metadata", {}).get("account") == "paper")
    paper_stats = None
    if paper_buys or paper_sells:
        paper_stats = {
            "buy_count": paper_buys,
            "sell_count": paper_sells,
            "net_pnl_cents": 0,  # 模拟盘盈亏需从 MX API 获取，暂用 0
        }

    # 8. 周报
    week_str = now.strftime("%Y-W%W")
    report = ctx.reporter.generate_weekly_report(week_str)

    # 9. Obsidian 周复盘
    ctx.obsidian.write_weekly_review({
        "week_str": week_str,
        "week_start": week_start_display,
        "week_end": week_end_display,
        "buy_count": buy_count,
        "sell_count": sell_count,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "profit_loss_ratio": profit_loss_ratio,
        "net_pnl_cents": net_pnl_cents,
        "total_profit_cents": total_profit,
        "total_loss_cents": total_loss,
        "trades": trades,
        "positions": pos_data,
        "core_pool": core_pool,
        "pool_changes": pool_changes,
        "paper_stats": paper_stats,
    })

    # 日志追加
    ctx.obsidian.write_daily_log(
        run_id,
        f"## 周报生成\n\n{week_str} 周报已生成。"
        f"{buy_count}买 {sell_count}卖 净盈亏¥{net_pnl_cents/100:+,.0f}",
    )

    _logger.info(
        f"[weekly] 完成: {buy_count}买 {sell_count}卖 "
        f"胜率{win_rate:.0%} 净盈亏¥{net_pnl_cents/100:+,.0f}"
    )

    # 10. Discord 推送
    try:
        from hermes.reporting.discord import format_weekly_embed
        from hermes.reporting.discord_sender import send_embed
        embed = format_weekly_embed({
            "week": week_str,
            "buy_count": buy_count, "sell_count": sell_count,
            "win_rate": win_rate, "profit_loss_ratio": profit_loss_ratio,
            "net_pnl_cents": net_pnl_cents,
            "positions": [{"name": p.name, "code": p.code, "shares": p.shares}
                          for p in positions],
        })
        ok, err = send_embed(embed)
        if not ok:
            _logger.warning(f"[weekly] Discord 推送失败: {err}")
    except Exception as e:
        _logger.warning(f"[weekly] Discord 推送异常: {e}")

    # 11. 月末自动生成月复盘
    _maybe_generate_monthly(ctx, run_id, now)

    return {
        "week": week_str,
        "buy_count": buy_count, "sell_count": sell_count,
        "win_rate": win_rate, "profit_loss_ratio": round(profit_loss_ratio, 2),
        "net_pnl_cents": net_pnl_cents,
    }


def _maybe_generate_monthly(ctx: PipelineContext, run_id: str, now: datetime):
    """如果是月末最后一周，自动生成月复盘。"""
    next_week = now + timedelta(days=7)
    if next_week.month != now.month:
        # 本周是本月最后一周，生成月复盘
        _generate_monthly_review(ctx, run_id, now)


def _generate_monthly_review(ctx: PipelineContext, run_id: str, now: datetime):
    """生成月复盘。"""
    month_str = now.strftime("%Y-%m")
    month_start = now.replace(day=1).strftime("%Y-%m-%d")
    month_start_utc, _ = local_date_bounds_utc(month_start)

    # 实盘统计
    filled_events = ctx.event_store.query(event_type="order.filled", since=month_start_utc)
    closed_events = ctx.event_store.query(event_type="position.closed", since=month_start_utc)

    buy_count = sum(1 for e in filled_events if e["payload"].get("side") == "buy")
    sell_count = sum(1 for e in filled_events if e["payload"].get("side") == "sell")

    wins = 0
    losses = 0
    total_profit = 0
    total_loss = 0
    worst_trades = []

    for e in closed_events:
        pnl = e["payload"].get("realized_pnl_cents", 0)
        if pnl > 0:
            wins += 1
            total_profit += pnl
        elif pnl < 0:
            losses += 1
            total_loss += abs(pnl)
            worst_trades.append({
                "code": e["payload"].get("code", ""),
                "name": e["payload"].get("name", ""),
                "pnl_cents": pnl,
                "date": iso_to_local(e.get("occurred_at", "")).date().isoformat(),
            })

    worst_trades.sort(key=lambda x: x["pnl_cents"])  # 最亏的排前面

    total_trades = wins + losses
    win_rate = wins / total_trades if total_trades > 0 else 0
    plr = (total_profit / total_loss) if total_loss > 0 else (
        float("inf") if total_profit > 0 else 0
    )
    net_pnl_cents = total_profit - total_loss
    avg_profit = total_profit // wins if wins > 0 else 0
    avg_loss = total_loss // losses if losses > 0 else 0

    # 周度汇总（按 ISO 周分组）
    weekly_map: dict[str, dict] = {}
    for e in filled_events:
        try:
            d = iso_to_local(e["occurred_at"])
            wk = d.strftime("%Y-W%W")
        except Exception:
            continue
        if wk not in weekly_map:
            weekly_map[wk] = {"week": wk, "pnl_cents": 0, "buy_count": 0,
                              "sell_count": 0, "wins": 0, "losses": 0}
        side = e["payload"].get("side", "")
        if side == "buy":
            weekly_map[wk]["buy_count"] += 1
        elif side == "sell":
            weekly_map[wk]["sell_count"] += 1

    for e in closed_events:
        try:
            d = iso_to_local(e["occurred_at"])
            wk = d.strftime("%Y-W%W")
        except Exception:
            continue
        if wk not in weekly_map:
            weekly_map[wk] = {"week": wk, "pnl_cents": 0, "buy_count": 0,
                              "sell_count": 0, "wins": 0, "losses": 0}
        pnl = e["payload"].get("realized_pnl_cents", 0)
        weekly_map[wk]["pnl_cents"] += pnl
        if pnl > 0:
            weekly_map[wk]["wins"] += 1
        elif pnl < 0:
            weekly_map[wk]["losses"] += 1

    weekly_summaries = sorted(weekly_map.values(), key=lambda x: x["week"])

    # 池子变动
    pool_demoted = ctx.event_store.query(event_type="pool.demoted", since=month_start_utc)
    pool_removed = ctx.event_store.query(event_type="pool.removed", since=month_start_utc)
    pool_changes = []
    for e in pool_demoted:
        p = e["payload"]
        pool_changes.append({
            "code": p.get("code", ""), "name": p.get("name", ""),
            "change_type": "demoted",
            "reason": f"降级: {p.get('reason', '')}",
            "date": iso_to_local(e.get("occurred_at", "")).date().isoformat(),
        })
    for e in pool_removed:
        p = e["payload"]
        pool_changes.append({
            "code": p.get("code", ""), "name": p.get("name", ""),
            "change_type": "removed",
            "reason": f"移出: 评分 {p.get('score', 0)}",
            "date": iso_to_local(e.get("occurred_at", "")).date().isoformat(),
        })

    # 模拟盘统计
    paper_events = ctx.event_store.query(event_type="auto_trade.executed", since=month_start_utc)
    paper_buys = sum(1 for e in paper_events
                     if e.get("payload", {}).get("side") == "buy"
                     and e.get("metadata", {}).get("account") == "paper")
    paper_sells = sum(1 for e in paper_events
                      if e.get("payload", {}).get("side") == "sell"
                      and e.get("metadata", {}).get("account") == "paper")
    paper_stats = None
    if paper_buys or paper_sells:
        paper_stats = {
            "buy_count": paper_buys,
            "sell_count": paper_sells,
            "net_pnl_cents": 0,
        }

    # 风控参数
    cfg = ctx.cfg
    risk_cfg = cfg.get("risk", {})
    pos_cfg = risk_cfg.get("position", {})
    momentum_cfg = risk_cfg.get("momentum", {})
    risk_params = {
        "stop_loss": f"{momentum_cfg.get('stop_loss', 0.08):.0%}",
        "trailing_stop": f"{momentum_cfg.get('trailing_stop', 0.10):.0%}",
        "time_stop_days": momentum_cfg.get("time_stop_days", 15),
        "weekly_max": pos_cfg.get("weekly_max", 2),
        "total_max": f"{pos_cfg.get('total_max', 0.60):.0%}",
        "single_max": f"{pos_cfg.get('single_max', 0.20):.0%}",
    }

    # 估算交易日数（工作日）
    month_start_dt = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    trading_days = sum(
        1 for i in range((now - month_start_dt).days + 1)
        if (month_start_dt + timedelta(days=i)).weekday() < 5
    )

    try:
        ctx.obsidian.write_monthly_review({
            "month_str": month_str,
            "trading_days": trading_days,
            "buy_count": buy_count,
            "sell_count": sell_count,
            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,
            "profit_loss_ratio": plr,
            "net_pnl_cents": net_pnl_cents,
            "total_profit_cents": total_profit,
            "total_loss_cents": total_loss,
            "max_drawdown_cents": 0,  # TODO: 从每日快照计算
            "avg_profit_cents": avg_profit,
            "avg_loss_cents": avg_loss,
            "weekly_summaries": weekly_summaries,
            "worst_trades": worst_trades,
            "pool_changes": pool_changes,
            "paper_stats": paper_stats,
            "risk_params": risk_params,
        })
        _logger.info(f"[weekly] 月复盘已生成: {month_str}")
    except Exception as e:
        _logger.warning(f"[weekly] 月复盘生成失败: {e}")
