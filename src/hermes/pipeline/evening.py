"""
pipeline/evening.py — 收盘报告

流程：
1. 抓大盘信号
2. 读持仓 + 检查风控（止损/止盈/时间止损）
3. 生成收盘报告 → report_artifacts
4. 写 Obsidian（日志 + 持仓概览）
5. 格式化 Discord embed
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date

from hermes.pipeline.context import PipelineContext
from hermes.pipeline.helpers import check_position_risks
from hermes.reporting.discord import format_evening_embed, format_stop_alert_embed

_logger = logging.getLogger(__name__)


def run(ctx: PipelineContext, run_id: str) -> dict:
    """执行收盘报告 pipeline。"""

    # 1. 大盘信号
    market_state = asyncio.run(ctx.market_svc.collect_market_state(run_id))
    signal = market_state.signal.value

    # 2. 持仓 + 风控（带 MA 数据 + 配置文件参数）
    positions = ctx.exec_svc.get_positions()
    risk_results = check_position_risks(ctx, positions, run_id)

    # check_position_risks 内部会通过 _update_position_price 把最新收盘价
    # 写入 projection_positions，但内存中的 positions 仍是旧快照。
    # 重新读一次，确保后续 Obsidian 日志 / Discord embed 用的是最新盈亏。
    positions = ctx.exec_svc.get_positions()

    risk_alerts = []
    stop_embeds = []

    for pos, signals in risk_results:
        # 写风控事件
        for s in signals:
            ctx.risk_svc._event_store.append(
                stream=f"risk:{pos.code}", stream_type="risk",
                event_type=f"risk.{s.signal_type}_triggered",
                payload={"code": pos.code, "signal_type": s.signal_type,
                         "trigger_price": s.trigger_price, "current_price": s.current_price,
                         "description": s.description, "urgency": s.urgency},
                metadata={"run_id": run_id},
            )
            risk_alerts.append(f"⚠️ {pos.name}({pos.code}): {s.description}")
            stop_embeds.append(format_stop_alert_embed({
                "code": pos.code, "signal_type": s.signal_type,
                "description": s.description, "urgency": s.urgency,
            }))

    # 3. 收盘报告
    report = ctx.reporter.generate_evening_report(run_id)

    # 4. Obsidian
    ctx.obsidian.write_portfolio_status()

    log_lines = [f"## 收盘报告", "", f"大盘信号: **{signal}**", ""]
    if positions:
        log_lines.append(f"持仓 {len(positions)} 只")
        for p in positions:
            pnl_pct = ((p.current_price or p.avg_cost) - p.avg_cost) / p.avg_cost * 100 if p.avg_cost else 0
            emoji = "🟢" if pnl_pct >= 0 else "🔴"
            log_lines.append(f"- {emoji} {p.name}({p.code}) {p.shares}股 盈亏 {pnl_pct:+.1f}%")
    else:
        log_lines.append("当前空仓")
    if risk_alerts:
        log_lines.extend(["", "### 风控触发"] + risk_alerts)
    ctx.obsidian.write_daily_log(run_id, "\n".join(log_lines))

    # 5. Discord embed
    discord_data = {
        "date": date.today().isoformat(),
        "market": market_state.detail.get("indices", {}),
        "positions": [{
            "name": p.name, "shares": p.shares,
            "pnl_pct": ((p.current_price or p.avg_cost) - p.avg_cost) / p.avg_cost * 100 if p.avg_cost else 0,
        } for p in positions],
        "alerts": risk_alerts,
    }
    embed = format_evening_embed(discord_data)

    _logger.info(f"[evening] 完成: {len(positions)} 持仓, {len(risk_alerts)} 风控触发")

    # 6. Discord 推送
    try:
        from hermes.reporting.discord_sender import send_embed
        ok, err = send_embed(embed)
        if not ok:
            _logger.warning(f"[evening] Discord 推送失败: {err}")
        # 止损告警单独推送
        for se in stop_embeds:
            send_embed(se, content="⚠️ 风控告警")
    except Exception as e:
        _logger.warning(f"[evening] Discord 推送异常: {e}")

    return {
        "signal": signal, "positions": len(positions),
        "risk_alerts": risk_alerts, "stop_embeds": stop_embeds,
        "discord_embed": embed,
    }
