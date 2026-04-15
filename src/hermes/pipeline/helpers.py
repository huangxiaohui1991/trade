"""
pipeline/helpers.py — Pipeline 共享工具函数

提供持仓风控检查等跨 pipeline 复用的逻辑。
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date
from typing import Optional

from hermes.pipeline.context import PipelineContext
from hermes.execution.models import Position
from hermes.risk.models import ExitSignal, RiskParams
from hermes.risk.rules import check_exit_signals, get_risk_params
from hermes.strategy.models import Style

_logger = logging.getLogger(__name__)


def _get_risk_cfg(ctx: PipelineContext) -> dict:
    """从配置中读取风控参数段。"""
    return ctx.cfg.get("risk", {})


def check_position_risks(
    ctx: PipelineContext,
    positions: list[Position],
    run_id: str,
) -> list[tuple[Position, list[ExitSignal]]]:
    """
    对持仓列表做风控检查，自动获取 MA 数据。

    Returns:
        [(position, [ExitSignal, ...]), ...]
    """
    if not positions:
        return []

    risk_cfg = _get_risk_cfg(ctx)

    # 批量获取持仓的技术指标（MA20/MA60）
    stock_list = [{"code": p.code, "name": p.name} for p in positions]
    try:
        snapshots = asyncio.run(ctx.market_svc.collect_batch(stock_list, run_id))
        ma_data = {}
        for snap in snapshots:
            if snap.technical:
                ma_data[snap.code] = {
                    "ma20": snap.technical.ma20,
                    "ma60": snap.technical.ma60,
                }
            # 同时更新持仓的 current_price 和 highest_since_entry
            if snap.quote and snap.quote.close > 0:
                _update_position_price(ctx, snap.code, snap.quote.close)
    except Exception as e:
        _logger.warning(f"[helpers] 批量获取 MA 数据失败: {e}")
        ma_data = {}

    results = []
    for pos in positions:
        style = Style(pos.style) if pos.style in ("slow_bull", "momentum") else Style.UNKNOWN
        params = get_risk_params(style, risk_cfg)
        try:
            entry_date = date.fromisoformat(pos.entry_date) if pos.entry_date else date.today()
        except ValueError:
            entry_date = date.today()

        ma_info = ma_data.get(pos.code, {})

        signals = check_exit_signals(
            code=pos.code,
            avg_cost=pos.avg_cost,
            current_price=pos.current_price or pos.avg_cost,
            entry_date=entry_date,
            today=date.today(),
            highest_since_entry=pos.highest_since_entry_cents / 100 if pos.highest_since_entry_cents else pos.avg_cost,
            entry_day_low=pos.entry_day_low_cents / 100 if pos.entry_day_low_cents else pos.avg_cost,
            params=params,
            ma20=ma_info.get("ma20", 0),
            ma60=ma_info.get("ma60", 0),
        )
        results.append((pos, signals))

    return results


def _update_position_price(ctx: PipelineContext, code: str, price: float):
    """更新持仓的 current_price 和 highest_since_entry。"""
    try:
        price_cents = int(price * 100)
        row = ctx.conn.execute(
            "SELECT highest_since_entry_cents, current_price_cents FROM projection_positions WHERE code = ?",
            (code,),
        ).fetchone()
        if not row:
            return
        old_highest = row["highest_since_entry_cents"] or 0
        new_highest = max(old_highest, price_cents)
        pnl = ctx.conn.execute(
            "SELECT avg_cost_cents, shares FROM projection_positions WHERE code = ?",
            (code,),
        ).fetchone()
        unrealized = 0
        if pnl:
            unrealized = (price_cents - pnl["avg_cost_cents"]) * pnl["shares"]

        ctx.conn.execute(
            """UPDATE projection_positions
               SET current_price_cents = ?,
                   highest_since_entry_cents = ?,
                   unrealized_pnl_cents = ?,
                   updated_at = datetime('now')
               WHERE code = ?""",
            (price_cents, new_highest, unrealized, code),
        )
    except Exception as e:
        _logger.warning(f"[helpers] 更新持仓价格失败 {code}: {e}")


def get_current_exposure(ctx: PipelineContext) -> tuple[float, int]:
    """
    计算当前仓位占比和本周买入次数。

    Returns:
        (current_exposure_pct, weekly_buy_count)
    """
    positions = ctx.exec_svc.get_positions()
    capital = ctx.capital

    total_market = sum(
        (p.current_price_cents or p.avg_cost_cents) * p.shares
        for p in positions
    )
    exposure_pct = total_market / (capital * 100) if capital > 0 else 0.0

    # 本周买入次数：查 event_log
    from datetime import datetime, timedelta
    today = datetime.now()
    # 本周一
    monday = today - timedelta(days=today.weekday())
    since = monday.strftime("%Y-%m-%d")

    buy_count = ctx.event_store.count(
        event_type="position.opened",
        since=since,
    )

    return exposure_pct, buy_count
