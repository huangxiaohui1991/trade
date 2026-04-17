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

from hermes.pipeline.context import PipelineContext
from hermes.pipeline.helpers import check_position_risks
from hermes.platform.time import local_today_str
from hermes.reporting.discord import format_evening_embed, format_combined_stop_alert_embed

_logger = logging.getLogger(__name__)


def _format_heatmap_markdown(sectors: list[dict]) -> list[str]:
    """把板块数据格式化为 markdown 表格（收盘版）。"""
    if not sectors:
        return ["数据获取失败"]
    # 成交额格式
    def fmt_amount(a: float) -> str:
        if a >= 1e8:
            return f"{a/1e8:.1f}亿"
        return f"{a/1e4:.0f}万"
    lines = []
    gainers = [s for s in sectors if s.get("change_pct", 0) > 0][:5]
    losers = [s for s in sectors if s.get("change_pct", 0) < 0][-5:]
    if gainers:
        lines.append("| 板块 | 涨跌幅 | 成交额 |")
        lines.append("|------|--------|--------|")
        for s in gainers:
            pct = s.get("change_pct", 0)
            lines.append(f"| 🔺 {s.get('name', '')} | `{pct:+.2f}%` | {fmt_amount(s.get('amount', 0))} |")
    if losers:
        lines.append("")
        lines.append("| 板块 | 涨跌幅 | 成交额 |")
        lines.append("|------|--------|--------|")
        for s in losers:
            pct = s.get("change_pct", 0)
            lines.append(f"| 🔻 {s.get('name', '')} | `{pct:+.2f}%` | {fmt_amount(s.get('amount', 0))} |")
    total_up = sum(s.get("up_count", 0) for s in sectors)
    total_down = sum(s.get("down_count", 0) for s in sectors)
    lines.append("")
    lines.append(f"*全市场 {len(sectors)} 个板块：上涨 **{total_up}** 个 / 下跌 **{total_down}** 个*")
    return lines


def run(ctx: PipelineContext, run_id: str) -> dict:
    """执行收盘报告 pipeline。"""

    # 1. 大盘信号
    market_state, index_data = asyncio.run(ctx.market_svc.collect_market_state(run_id))
    signal = market_state.signal.value

    # 同步指数数据到 projection_market_state 表
    if index_data:
        ctx.projector.sync_market_state(index_data)

    # 2. 持仓 + 风控（带 MA 数据 + 配置文件参数）
    # 先刷新持仓收盘价
    from hermes.pipeline.helpers import refresh_position_prices
    refresh_position_prices(ctx)

    positions = ctx.exec_svc.get_positions()
    risk_results = check_position_risks(ctx, positions, run_id)

    # check_position_risks 内部会通过 _update_position_price 把最新收盘价
    # 写入 projection_positions，但内存中的 positions 仍是旧快照。
    # 重新读一次，确保后续 Obsidian 日志 / Discord embed 用的是最新盈亏。
    positions = ctx.exec_svc.get_positions()

    risk_alerts = []
    stop_signals = []

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
            stop_signals.append({
                "code": pos.code, "signal_type": s.signal_type,
                "description": s.description, "urgency": s.urgency,
            })

    # 3. 收盘报告
    report = ctx.reporter.generate_evening_report(run_id)

    # 4. Obsidian
    ctx.obsidian.write_portfolio_status()
    ctx.obsidian.write_account_overview()

    # 信号快照（收盘后生成当日完整快照）
    ctx.obsidian.write_signal_snapshot(
        run_id=run_id,
        market_state_detail=market_state.detail,
        market_signal=signal,
    )
    # 当日输出索引（聚合当日所有 pipeline 运行）
    ctx.obsidian.write_daily_output_index(run_id)
    # 候选池总览
    ctx.obsidian.write_candidate_pool_overview()
    # 决策池
    ctx.obsidian.write_decision_pool()

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

    # 5. 行业热力图
    heatmap_sectors = asyncio.run(ctx.market_svc.collect_sector_heatmap())
    _logger.info(f"[evening] 行业热力图: {len(heatmap_sectors)} 个板块")
    if heatmap_sectors:
        heatmap_lines = _format_heatmap_markdown(heatmap_sectors)
        log_lines.extend(["", "### 行业热力图"] + heatmap_lines)
    else:
        log_lines.extend(["", "### 行业热力图", "数据获取失败"])

    ctx.obsidian.write_daily_log(run_id, "\n".join(log_lines))

    # 6. Discord embed
    discord_data = {
        "date": local_today_str(),
        "market": market_state.detail.get("indices", {}),
        "positions": [{
            "name": p.name, "shares": p.shares,
            "pnl_pct": ((p.current_price or p.avg_cost) - p.avg_cost) / p.avg_cost * 100 if p.avg_cost else 0,
        } for p in positions],
        "alerts": risk_alerts,
    }
    embed = format_evening_embed(discord_data)

    _logger.info(f"[evening] 完成: {len(positions)} 持仓, {len(risk_alerts)} 风控触发")

    # 7. Discord 推送
    try:
        from hermes.reporting.discord import format_sector_heatmap_embed
        from hermes.reporting.discord_sender import send_embed
        ok, err = send_embed(embed)
        if not ok:
            _logger.warning(f"[evening] Discord 推送失败: {err}")
        # 风控告警合并为单张卡片推送
        if stop_signals:
            combined_alert = format_combined_stop_alert_embed(stop_signals)
            send_embed(combined_alert, content="⚠️ 风控告警")
        # 行业热力图
        heatmap_embed = format_sector_heatmap_embed(heatmap_sectors, title="收盘")
        ok2, err2 = send_embed(heatmap_embed)
        if not ok2:
            _logger.warning(f"[evening] 热力图 Discord 推送失败: {err2}")
    except Exception as e:
        _logger.warning(f"[evening] Discord 推送异常: {e}")

    return {
        "signal": signal, "positions": len(positions),
        "risk_alerts": risk_alerts, "stop_signals": stop_signals,
        "discord_embed": embed,
    }
