"""
pipeline/morning.py — 盘前摘要

流程：
1. 抓大盘信号
2. 读持仓 + 检查风控
3. 读核心池状态
4. 生成今日决策
5. 生成盘前报告 → report_artifacts
6. 写 Obsidian（日志 + 今日决策 + 持仓概览）
7. 格式化 Discord embed
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date
from typing import Optional

from hermes.pipeline.context import PipelineContext
from hermes.risk.rules import check_exit_signals, get_risk_params
from hermes.strategy.models import Style
from hermes.reporting.discord import format_morning_embed

_logger = logging.getLogger(__name__)


def run(ctx: PipelineContext, run_id: str) -> dict:
    """执行盘前摘要 pipeline。"""

    # 1. 大盘信号
    market_state = asyncio.run(ctx.market_svc.collect_market_state(run_id))
    signal = market_state.signal.value
    multiplier = market_state.multiplier
    _logger.info(f"[morning] 大盘信号: {signal} (multiplier={multiplier})")

    # 2. 持仓 + 风控
    positions = ctx.exec_svc.get_positions()
    risk_alerts = []
    for pos in positions:
        style = Style(pos.style) if pos.style in ("slow_bull", "momentum") else Style.UNKNOWN
        params = get_risk_params(style)
        try:
            entry_date = date.fromisoformat(pos.entry_date) if pos.entry_date else date.today()
        except ValueError:
            entry_date = date.today()
        signals = check_exit_signals(
            code=pos.code, avg_cost=pos.avg_cost,
            current_price=pos.current_price or pos.avg_cost,
            entry_date=entry_date, today=date.today(),
            highest_since_entry=pos.highest_since_entry_cents / 100 if pos.highest_since_entry_cents else pos.avg_cost,
            entry_day_low=pos.entry_day_low_cents / 100 if pos.entry_day_low_cents else pos.avg_cost,
            params=params,
        )
        for s in signals:
            risk_alerts.append(f"⚠️ {pos.name}({pos.code}): {s.description} [{s.urgency}]")

    # 3. 核心池
    pool_rows = ctx.conn.execute(
        "SELECT code, name, score FROM projection_candidate_pool WHERE pool_tier = 'core' ORDER BY score DESC"
    ).fetchall()
    core_pool = [{"name": r["name"] or r["code"], "code": r["code"], "score": r["score"] or 0} for r in pool_rows]

    # 4. 今日决策
    can_buy = signal in ("GREEN", "YELLOW") and len(risk_alerts) == 0
    reasons = [f"market_signal={signal}"]
    if risk_alerts:
        reasons.extend(risk_alerts)

    if signal in ("RED", "CLEAR"):
        decision_action = "NO_TRADE"
    elif not can_buy:
        decision_action = "NO_TRADE"
    elif signal == "YELLOW":
        decision_action = "REDUCED_BUY"
    else:
        decision_action = "BUY_ALLOWED"

    ctx.obsidian.write_today_decision(
        market_signal=signal, multiplier=multiplier,
        can_buy=can_buy, holding_count=len(positions),
        exposure_pct=0.0, reasons=reasons,
    )

    # 5. 盘前报告
    report = ctx.reporter.generate_morning_report(run_id)

    # 6. Obsidian
    ctx.obsidian.write_portfolio_status()
    log_lines = [f"## 盘前摘要", f"", f"大盘信号: **{signal}** (仓位系数 {multiplier})", ""]
    if positions:
        log_lines.append(f"持仓 {len(positions)} 只")
        for p in positions:
            log_lines.append(f"- {p.name}({p.code}) {p.shares}股 成本¥{p.avg_cost:.2f}")
    else:
        log_lines.append("当前空仓")
    if risk_alerts:
        log_lines.extend(["", "### 风控预警"] + risk_alerts)
    if core_pool:
        log_lines.extend(["", "### 核心池"])
        for s in core_pool[:5]:
            emoji = "✅" if s["score"] >= 7 else ("🟡" if s["score"] >= 5 else "❌")
            log_lines.append(f"- {s['name']} {emoji} {s['score']:.1f}")
    ctx.obsidian.write_daily_log(run_id, "\n".join(log_lines))

    # 7. Discord embed
    discord_data = {
        "date": date.today().isoformat(),
        "market_signal": signal,
        "market": market_state.detail.get("indices", {}),
        "positions": [{"name": p.name, "shares": p.shares, "price": p.current_price or p.avg_cost} for p in positions],
        "core_pool": core_pool[:5],
        "decision": {
            "action": decision_action,
            "multiplier": multiplier,
            "holding_count": len(positions),
            "risk_alerts": risk_alerts,
        },
    }
    embed = format_morning_embed(discord_data)

    _logger.info(f"[morning] 完成: {len(positions)} 持仓, {len(core_pool)} 核心池, {len(risk_alerts)} 风控预警")

    # 8. Discord 推送
    try:
        from hermes.reporting.discord_sender import send_embed
        ok, err = send_embed(embed)
        if not ok:
            _logger.warning(f"[morning] Discord 推送失败: {err}")
    except Exception as e:
        _logger.warning(f"[morning] Discord 推送异常: {e}")

    return {
        "signal": signal, "multiplier": multiplier,
        "positions": len(positions), "core_pool": len(core_pool),
        "risk_alerts": risk_alerts, "discord_embed": embed,
    }
