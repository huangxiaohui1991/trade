"""
pipeline/auto_trade.py — 模拟盘自动交易

基于公共选股池的评分结果，自动在妙想模拟盘执行买卖。
持仓/资金以 MX API 为 source of truth，不污染实盘数据。

流程：
1. 查模拟盘持仓 + 资金（MX API）
2. 大盘择时信号
3. 风控检查（对模拟盘持仓）→ 自动卖出
4. 读公共池评分 → 决策 → 自动买入
5. 事件记录（account=paper）+ Discord 推送
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timezone

from hermes.pipeline.context import PipelineContext
from hermes.pipeline.paper_account import PaperAccount, PaperPosition, PaperBalance
from hermes.strategy.models import Action, MarketSignal, Style
from hermes.risk.rules import check_exit_signals, get_risk_params
from hermes.risk.models import RiskParams

_logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_auto_trade_cfg(ctx: PipelineContext) -> dict:
    """读取 auto_trade 配置段。"""
    return ctx.cfg.get("auto_trade", {})


def _calc_buy_shares(price: float, cash: float, position_pct: float, total_asset: float) -> int:
    """
    计算买入股数（100 的整数倍）。

    position_pct: 目标仓位占比（如 0.10）
    """
    if price <= 0 or total_asset <= 0:
        return 0
    target_amount = total_asset * position_pct
    max_by_cash = cash * 0.95  # 留 5% 余量
    amount = min(target_amount, max_by_cash)
    shares = int(amount / price / 100) * 100
    return max(shares, 0)


def run(ctx: PipelineContext, run_id: str) -> dict:
    """执行模拟盘自动交易 pipeline。"""

    cfg = _get_auto_trade_cfg(ctx)
    if not cfg.get("enabled", False):
        _logger.info("[auto_trade] 未启用，跳过")
        return {"enabled": False, "buys": [], "sells": []}

    dry_run = cfg.get("dry_run", True)
    max_daily_trades = cfg.get("max_daily_trades", 4)
    paper = PaperAccount()

    # ------------------------------------------------------------------
    # 1. 查模拟盘状态
    # ------------------------------------------------------------------
    positions = paper.get_positions()
    balance = paper.get_balance()
    exposure_pct, available_cash = paper.get_exposure()

    _logger.info(
        f"[auto_trade] 模拟盘: {len(positions)} 持仓, "
        f"总资产 ¥{balance.total_asset:,.0f}, 可用 ¥{available_cash:,.0f}, "
        f"仓位 {exposure_pct:.1%}"
    )

    # ------------------------------------------------------------------
    # 2. 大盘信号
    # ------------------------------------------------------------------
    market_state = asyncio.run(ctx.market_svc.collect_market_state(run_id))
    signal = market_state.signal
    _logger.info(f"[auto_trade] 大盘信号: {signal.value}")

    sells: list[dict] = []
    buys: list[dict] = []
    trade_count = 0

    # ------------------------------------------------------------------
    # 3. 风控检查 → 自动卖出
    # ------------------------------------------------------------------
    sells = _check_and_sell(ctx, paper, positions, market_state, run_id, cfg, dry_run)
    trade_count += len(sells)

    # ------------------------------------------------------------------
    # 4. 评分决策 → 自动买入
    # ------------------------------------------------------------------
    if trade_count < max_daily_trades:
        # 刷新资金（卖出后可能变化）
        if sells:
            balance = paper.get_balance()
            exposure_pct, available_cash = paper.get_exposure()

        remaining_trades = max_daily_trades - trade_count
        buys = _score_and_buy(
            ctx, paper, balance, exposure_pct, available_cash,
            market_state, run_id, cfg, dry_run, remaining_trades,
        )

    # ------------------------------------------------------------------
    # 5. 汇总 + Discord 推送
    # ------------------------------------------------------------------
    _record_summary_event(ctx, run_id, buys, sells, dry_run)

    embed = _format_auto_trade_embed(buys, sells, balance, market_state, dry_run)
    try:
        from hermes.reporting.discord_sender import send_embed
        prefix = "🧪 " if dry_run else ""
        ok, err = send_embed(embed, content=f"{prefix}模拟盘自动交易")
        if not ok:
            _logger.warning(f"[auto_trade] Discord 推送失败: {err}")
    except Exception as e:
        _logger.warning(f"[auto_trade] Discord 推送异常: {e}")

    # Obsidian 日志 + 模拟盘日报
    _write_obsidian_log(ctx, run_id, buys, sells, dry_run)

    # 刷新最新持仓/资金（交易后可能变化）
    final_positions = paper.get_positions()
    final_balance = paper.get_balance()

    # 写模拟盘完整日报
    ctx.obsidian.write_paper_report(
        run_id=run_id,
        positions=final_positions,
        balance={
            "total_asset": final_balance.total_asset,
            "available_cash": final_balance.available_cash,
            "market_value": final_balance.market_value,
        },
        buys=buys,
        sells=sells,
        market_signal=signal.value,
        market_indices=market_state.detail.get("indices", {}),
        dry_run=dry_run,
    )

    # 追加交易记录
    trade_rows = []
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    for s in sells:
        trade_rows.append({
            "time": now_str, "side": "sell",
            "name": s.get("name", ""), "code": s.get("code", ""),
            "shares": s.get("shares", 0), "price": s.get("price", 0),
            "amount": s.get("shares", 0) * s.get("price", 0),
            "reason": f"[{s.get('reason', '')}] {s.get('risk_description', '')}".strip(),
        })
    for b in buys:
        trade_rows.append({
            "time": now_str, "side": "buy",
            "name": b.get("name", ""), "code": b.get("code", ""),
            "shares": b.get("shares", 0), "price": b.get("price", 0),
            "amount": b.get("shares", 0) * b.get("price", 0),
            "reason": f"[BUY_CORE_POOL] 评分 {b.get('score', 0):.1f}",
        })
    if trade_rows:
        ctx.obsidian.append_paper_trade_log(trade_rows)

    # 刷新当日输出索引
    ctx.obsidian.write_daily_output_index(run_id)

    result = {
        "enabled": True,
        "dry_run": dry_run,
        "signal": signal.value,
        "paper_positions": len(positions),
        "paper_total_asset": balance.total_asset,
        "buys": buys,
        "sells": sells,
        "discord_embed": embed,
    }
    _logger.info(f"[auto_trade] 完成: {len(buys)} 买入, {len(sells)} 卖出, dry_run={dry_run}")
    return result


# ======================================================================
# 卖出逻辑
# ======================================================================

def _check_and_sell(
    ctx: PipelineContext,
    paper: PaperAccount,
    positions: list[PaperPosition],
    market_state,
    run_id: str,
    cfg: dict,
    dry_run: bool,
) -> list[dict]:
    """对模拟盘持仓做风控检查，触发则自动卖出。"""
    if not positions:
        return []

    risk_cfg = ctx.cfg.get("risk", {})
    sells = []

    # 批量获取 MA 数据
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
    except Exception as e:
        _logger.warning(f"[auto_trade] 批量获取 MA 数据失败: {e}")
        ma_data = {}

    # 大盘 CLEAR 信号 → 全部卖出
    if market_state.signal == MarketSignal.CLEAR:
        _logger.info("[auto_trade] 大盘 CLEAR，清仓所有模拟盘持仓")
        for pos in positions:
            if pos.shares <= 0:
                continue
            sell_info = _execute_sell(paper, pos, "market_clear", run_id, ctx, dry_run)
            if sell_info:
                sells.append(sell_info)
        return sells

    for pos in positions:
        if pos.shares <= 0:
            continue

        # 推断风格（默认 momentum，模拟盘偏短线）
        style = Style.MOMENTUM
        ma_info = ma_data.get(pos.code, {})

        params = get_risk_params(style, risk_cfg)

        # 估算 entry_date（MX API 不提供，用 30 天前近似）
        # TODO: 从事件日志中查找实际买入日期
        entry_date = date.today()
        paper_events = ctx.event_store.query(
            event_type="auto_trade.executed",
            stream=f"paper:{pos.code}",
        )
        for ev in reversed(paper_events):
            p = ev.get("payload", {})
            if p.get("side") == "buy":
                try:
                    entry_date = date.fromisoformat(ev["occurred_at"][:10])
                except (ValueError, KeyError):
                    pass
                break

        signals = check_exit_signals(
            code=pos.code,
            avg_cost=pos.avg_cost,
            current_price=pos.current_price,
            entry_date=entry_date,
            today=date.today(),
            highest_since_entry=pos.current_price,  # MX 不提供最高价，用现价近似
            entry_day_low=pos.avg_cost,
            params=params,
            ma20=ma_info.get("ma20", 0),
            ma60=ma_info.get("ma60", 0),
        )

        # 只对 immediate 级别自动卖出
        immediate = [s for s in signals if s.urgency == "immediate"]
        if immediate:
            reason = immediate[0].signal_type
            desc = immediate[0].description
            _logger.info(f"[auto_trade] 风控触发卖出 {pos.name}({pos.code}): {desc}")
            sell_info = _execute_sell(paper, pos, reason, run_id, ctx, dry_run)
            if sell_info:
                sell_info["risk_description"] = desc
                sells.append(sell_info)

    return sells


def _execute_sell(
    paper: PaperAccount,
    pos: PaperPosition,
    reason: str,
    run_id: str,
    ctx: PipelineContext,
    dry_run: bool,
) -> dict | None:
    """执行模拟盘卖出。"""
    info = {
        "side": "sell",
        "code": pos.code,
        "name": pos.name,
        "shares": pos.shares,
        "price": pos.current_price,
        "reason": reason,
        "dry_run": dry_run,
    }

    if dry_run:
        _logger.info(f"[auto_trade][DRY] 卖出 {pos.name}({pos.code}) {pos.shares}股")
        info["status"] = "dry_run"
    else:
        result = paper.sell(pos.code, pos.shares)
        if result.success:
            info["status"] = "filled"
            info["order_id"] = result.order_id
            _logger.info(f"[auto_trade] 卖出成功 {pos.name}({pos.code}) {pos.shares}股")
        else:
            _logger.warning(f"[auto_trade] 卖出失败 {pos.name}({pos.code}): {result.error}")
            info["status"] = "failed"
            info["error"] = result.error

    # 记录事件
    ctx.event_store.append(
        stream=f"paper:{pos.code}",
        stream_type="paper_trade",
        event_type="auto_trade.executed",
        payload=info,
        metadata={"run_id": run_id, "account": "paper"},
    )
    return info


# ======================================================================
# 买入逻辑
# ======================================================================

def _score_and_buy(
    ctx: PipelineContext,
    paper: PaperAccount,
    balance: PaperBalance,
    exposure_pct: float,
    available_cash: float,
    market_state,
    run_id: str,
    cfg: dict,
    dry_run: bool,
    max_trades: int,
) -> list[dict]:
    """从公共池读取评分，决策后自动买入。"""

    # 大盘 RED/CLEAR 禁止买入
    if market_state.signal in (MarketSignal.RED, MarketSignal.CLEAR):
        _logger.info(f"[auto_trade] 大盘 {market_state.signal.value}，禁止买入")
        return []

    # 仓位上限
    pos_cfg = ctx.cfg.get("risk", {}).get("position", {})
    total_max = pos_cfg.get("total_max", 0.60)
    single_max = pos_cfg.get("single_max", 0.20)

    if exposure_pct >= total_max:
        _logger.info(f"[auto_trade] 仓位 {exposure_pct:.1%} >= {total_max:.0%}，禁止买入")
        return []

    # 本周模拟盘买入次数
    from datetime import timedelta
    today = datetime.now()
    monday = today - timedelta(days=today.weekday())
    since = monday.strftime("%Y-%m-%d")
    weekly_events = ctx.event_store.query(
        event_type="auto_trade.executed",
        since=since,
    )
    weekly_buy_count = sum(
        1 for ev in weekly_events
        if ev.get("payload", {}).get("side") == "buy"
        and ev.get("payload", {}).get("status") in ("filled", "dry_run")
        and ev.get("metadata", {}).get("account") == "paper"
    )

    weekly_max = pos_cfg.get("weekly_max", 2)
    if weekly_buy_count >= weekly_max:
        _logger.info(f"[auto_trade] 本周已买 {weekly_buy_count}/{weekly_max}，禁止买入")
        return []

    # 读公共池评分（最近一次 scoring pipeline 的结果）
    candidates = _get_buy_candidates(ctx, run_id, market_state, exposure_pct, weekly_buy_count)

    if not candidates:
        _logger.info("[auto_trade] 无符合条件的买入候选")
        return []

    # 已持有的模拟盘股票
    paper_positions = paper.get_positions()
    held_codes = {p.code for p in paper_positions}

    buys = []
    remaining = min(max_trades, weekly_max - weekly_buy_count)

    for candidate in candidates:
        if remaining <= 0:
            break

        code = candidate["code"]
        if code in held_codes:
            continue

        # 计算仓位
        position_pct = min(single_max * market_state.multiplier, total_max - exposure_pct)
        if position_pct <= 0.01:
            break

        price = candidate.get("price", 0)
        if price <= 0:
            continue

        shares = _calc_buy_shares(price, available_cash, position_pct, balance.total_asset)
        if shares <= 0:
            continue

        buy_info = _execute_buy(paper, code, candidate.get("name", code), shares, price, run_id, ctx, dry_run)
        if buy_info:
            buy_info["score"] = candidate.get("score", 0)
            buy_info["position_pct"] = position_pct
            buys.append(buy_info)
            remaining -= 1
            # 更新可用资金估算
            available_cash -= shares * price
            exposure_pct += position_pct

    return buys


def _get_buy_candidates(
    ctx: PipelineContext,
    run_id: str,
    market_state,
    exposure_pct: float,
    weekly_buy_count: int,
) -> list[dict]:
    """
    从公共池获取买入候选。

    优先用最近 scoring pipeline 的决策事件，
    fallback 到核心池评分 + 实时决策。
    """
    # 方式 1：查最近的 decision.suggested 事件中 action=BUY 的
    recent_decisions = ctx.event_store.query(
        event_type="decision.suggested",
        limit=50,
    )

    candidates = []
    seen = set()
    today_str = date.today().isoformat()

    for ev in recent_decisions:
        # 只取今天的决策
        if not ev.get("occurred_at", "").startswith(today_str):
            continue
        p = ev.get("payload", {})
        if p.get("action") != "BUY":
            continue
        code = p.get("code", "")
        if code in seen:
            continue
        seen.add(code)
        candidates.append({
            "code": code,
            "name": p.get("name", code),
            "score": p.get("score", 0),
            "position_pct": p.get("position_pct", 0),
            "price": 0,  # 需要实时获取
        })

    if not candidates:
        # 方式 2：从核心池取高分股票
        rows = ctx.conn.execute(
            "SELECT code, name, score FROM projection_candidate_pool "
            "WHERE pool_tier = 'core' AND score >= ? ORDER BY score DESC LIMIT 10",
            (ctx.cfg.get("scoring", {}).get("thresholds", {}).get("buy", 6.5),),
        ).fetchall()
        for r in rows:
            candidates.append({
                "code": r["code"],
                "name": r["name"] or r["code"],
                "score": r["score"] or 0,
                "price": 0,
            })

    if not candidates:
        return []

    # 获取实时价格
    stock_list = [{"code": c["code"], "name": c["name"]} for c in candidates]
    try:
        snapshots = asyncio.run(ctx.market_svc.collect_batch(stock_list, run_id))
        price_map = {}
        for snap in snapshots:
            if snap.quote and snap.quote.close > 0:
                price_map[snap.code] = snap.quote.close
        for c in candidates:
            c["price"] = price_map.get(c["code"], 0)
    except Exception as e:
        _logger.warning(f"[auto_trade] 获取实时价格失败: {e}")

    # 过滤无价格的
    candidates = [c for c in candidates if c["price"] > 0]
    # 按评分降序
    candidates.sort(key=lambda c: c.get("score", 0), reverse=True)

    return candidates


def _execute_buy(
    paper: PaperAccount,
    code: str,
    name: str,
    shares: int,
    price: float,
    run_id: str,
    ctx: PipelineContext,
    dry_run: bool,
) -> dict | None:
    """执行模拟盘买入。"""
    info = {
        "side": "buy",
        "code": code,
        "name": name,
        "shares": shares,
        "price": price,
        "amount": shares * price,
        "dry_run": dry_run,
    }

    if dry_run:
        _logger.info(f"[auto_trade][DRY] 买入 {name}({code}) {shares}股 @ ¥{price:.2f}")
        info["status"] = "dry_run"
    else:
        result = paper.buy(code, shares)
        if result.success:
            info["status"] = "filled"
            info["order_id"] = result.order_id
            _logger.info(f"[auto_trade] 买入成功 {name}({code}) {shares}股 @ ¥{price:.2f}")
        else:
            _logger.warning(f"[auto_trade] 买入失败 {name}({code}): {result.error}")
            info["status"] = "failed"
            info["error"] = result.error

    # 记录事件
    ctx.event_store.append(
        stream=f"paper:{code}",
        stream_type="paper_trade",
        event_type="auto_trade.executed",
        payload=info,
        metadata={"run_id": run_id, "account": "paper"},
    )
    return info


# ======================================================================
# 报告
# ======================================================================

def _record_summary_event(ctx: PipelineContext, run_id: str, buys: list, sells: list, dry_run: bool):
    """记录自动交易汇总事件。"""
    ctx.event_store.append(
        stream="paper:summary",
        stream_type="paper_trade",
        event_type="auto_trade.summary",
        payload={
            "date": date.today().isoformat(),
            "dry_run": dry_run,
            "buy_count": len(buys),
            "sell_count": len(sells),
            "buys": buys,
            "sells": sells,
        },
        metadata={"run_id": run_id, "account": "paper"},
    )


def _format_auto_trade_embed(
    buys: list, sells: list, balance: PaperBalance, market_state, dry_run: bool,
) -> dict:
    """格式化 Discord embed。"""
    from hermes.reporting.discord import _embed, _field, SIGNAL_EMOJI, COLORS

    date_str = date.today().isoformat()
    sig = market_state.signal.value
    sig_emoji = SIGNAL_EMOJI.get(sig, "")
    title_prefix = "🧪 " if dry_run else "🤖 "
    mode = "[模拟]" if dry_run else ""

    fields = [
        _field("大盘", f"{sig_emoji} {sig}"),
        _field("总资产", f"¥{balance.total_asset:,.0f}"),
        _field("可用资金", f"¥{balance.available_cash:,.0f}"),
    ]

    if sells:
        sell_lines = []
        for s in sells:
            status = "✅" if s.get("status") == "filled" else ("🧪" if s.get("status") == "dry_run" else "❌")
            reason = s.get("reason", "")
            sell_lines.append(f"{status} {s['name']}({s['code']}) {s['shares']}股 | {reason}")
        fields.append(_field(f"🔴 卖出{mode}（{len(sells)}）", "\n".join(sell_lines), inline=False))

    if buys:
        buy_lines = []
        for b in buys:
            status = "✅" if b.get("status") == "filled" else ("🧪" if b.get("status") == "dry_run" else "❌")
            score = b.get("score", 0)
            buy_lines.append(
                f"{status} {b['name']}({b['code']}) {b['shares']}股 "
                f"@ ¥{b['price']:.2f} | 评分 {score:.1f}"
            )
        fields.append(_field(f"🟢 买入{mode}（{len(buys)}）", "\n".join(buy_lines), inline=False))

    if not buys and not sells:
        fields.append(_field("📋 操作", "无交易信号", inline=False))

    return _embed(
        title=f"{title_prefix}模拟盘自动交易 — {date_str}",
        color=COLORS.get("info", 0x37474F),
        fields=fields,
        footer="Hermes · auto_trade · paper",
    )


def _write_obsidian_log(ctx: PipelineContext, run_id: str, buys: list, sells: list, dry_run: bool):
    """写 Obsidian 日志。"""
    mode = "[DRY RUN] " if dry_run else ""
    lines = [f"## {mode}模拟盘自动交易", ""]

    if sells:
        lines.append("### 卖出")
        for s in sells:
            status = s.get("status", "")
            lines.append(f"- 🔴 {s['name']}({s['code']}) {s['shares']}股 | {s.get('reason', '')} [{status}]")
        lines.append("")

    if buys:
        lines.append("### 买入")
        for b in buys:
            status = b.get("status", "")
            lines.append(
                f"- 🟢 {b['name']}({b['code']}) {b['shares']}股 "
                f"@ ¥{b['price']:.2f} | 评分 {b.get('score', 0):.1f} [{status}]"
            )
        lines.append("")

    if not buys and not sells:
        lines.append("无交易信号")

    ctx.obsidian.write_daily_log(run_id, "\n".join(lines))
