"""
pipeline/noon.py — 午休检查

流程：
1. 读持仓 + 当前价格
2. 检查风控（止损/止盈是否接近触发）
3. 检查是否有加仓机会（核心池高分 + 大盘 GREEN）
4. 写 Obsidian 日志
5. 格式化 Discord embed 推送
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date

from hermes.pipeline.context import PipelineContext
from hermes.risk.rules import check_exit_signals, get_risk_params
from hermes.strategy.models import Style
from hermes.reporting.discord import _embed, _field, COLORS

_logger = logging.getLogger(__name__)


def _format_noon_embed(data: dict) -> dict:
    """午休检查 → Discord embed。"""
    date_str = data.get("date", "")
    fields = []

    # 大盘
    signal = data.get("signal", "")
    fields.append(_field("大盘信号", f"**{signal}**"))

    # 持仓
    positions = data.get("positions", [])
    if positions:
        for p in positions:
            pnl = p.get("pnl_pct", 0)
            emoji = "🟢" if pnl >= 0 else "🔴"
            fields.append(_field(
                f"{emoji} {p['name']}",
                f"{p['shares']}股 · 成本 ¥{p['cost']:.2f}\n现价 ¥{p['price']:.2f} · 盈亏 **{pnl:+.2f}%**",
            ))
    else:
        fields.append(_field("持仓", "空仓"))

    # 风控
    alerts = data.get("alerts", [])
    if alerts:
        fields.append(_field(f"⚠️ 风控提示（{len(alerts)}）", "\n".join(f"• {a}" for a in alerts), inline=False))

    # 提示
    tips = data.get("tips", [])
    if tips:
        fields.append(_field("📋 提示", "\n".join(f"• {t}" for t in tips), inline=False))

    return _embed(
        title=f"☀️ 午休检查 — {date_str}",
        color=COLORS["noon"],
        fields=fields,
        footer="Hermes · noon_check",
    )


def run(ctx: PipelineContext, run_id: str) -> dict:
    """执行午休检查 pipeline。"""

    # 1. 大盘
    market_state = asyncio.run(ctx.market_svc.collect_market_state(run_id))
    signal = market_state.signal.value

    # 2. 持仓 + 风控
    positions = ctx.exec_svc.get_positions()
    alerts = []
    pos_data = []

    for pos in positions:
        style = Style(pos.style) if pos.style in ("slow_bull", "momentum") else Style.UNKNOWN
        params = get_risk_params(style)
        try:
            entry_date = date.fromisoformat(pos.entry_date) if pos.entry_date else date.today()
        except ValueError:
            entry_date = date.today()

        current = pos.current_price or pos.avg_cost
        pnl_pct = (current - pos.avg_cost) / pos.avg_cost * 100 if pos.avg_cost else 0

        signals = check_exit_signals(
            code=pos.code, avg_cost=pos.avg_cost,
            current_price=current,
            entry_date=entry_date, today=date.today(),
            highest_since_entry=pos.highest_since_entry_cents / 100 if pos.highest_since_entry_cents else pos.avg_cost,
            entry_day_low=pos.entry_day_low_cents / 100 if pos.entry_day_low_cents else pos.avg_cost,
            params=params,
        )
        for s in signals:
            alerts.append(f"{pos.name}({pos.code}): {s.description}")

        pos_data.append({
            "name": pos.name, "code": pos.code, "shares": pos.shares,
            "cost": pos.avg_cost, "price": current, "pnl_pct": pnl_pct,
        })

    # 3. 加仓提示
    tips = []
    if signal in ("GREEN", "YELLOW"):
        pool_rows = ctx.conn.execute(
            "SELECT code, name, score FROM projection_candidate_pool WHERE pool_tier = 'core' AND score >= 7 ORDER BY score DESC LIMIT 3"
        ).fetchall()
        for r in pool_rows:
            tips.append(f"{r['name']}({r['code']}) 评分 {r['score']:.1f}，可关注加仓")
    if signal in ("RED", "CLEAR"):
        tips.append(f"大盘 {signal}，不建议操作")

    # 4. Obsidian 日志
    log_lines = ["## 午休检查", "", f"大盘: **{signal}**", ""]
    if pos_data:
        for p in pos_data:
            emoji = "🟢" if p["pnl_pct"] >= 0 else "🔴"
            log_lines.append(f"- {emoji} {p['name']} {p['pnl_pct']:+.1f}%")
    if alerts:
        log_lines.extend(["", "### 风控提示"] + [f"- ⚠️ {a}" for a in alerts])
    if tips:
        log_lines.extend(["", "### 操作提示"] + [f"- {t}" for t in tips])
    ctx.obsidian.write_daily_log(run_id, "\n".join(log_lines))

    # 5. Discord
    embed = _format_noon_embed({
        "date": date.today().isoformat(), "signal": signal,
        "positions": pos_data, "alerts": alerts, "tips": tips,
    })

    try:
        from hermes.reporting.discord_sender import send_embed
        ok, err = send_embed(embed)
        if not ok:
            _logger.warning(f"[noon] Discord 推送失败: {err}")
    except Exception as e:
        _logger.warning(f"[noon] Discord 推送异常: {e}")

    _logger.info(f"[noon] 完成: {signal}, {len(positions)} 持仓, {len(alerts)} 风控")

    return {
        "signal": signal, "positions": len(positions),
        "alerts": alerts, "tips": tips, "discord_embed": embed,
    }
