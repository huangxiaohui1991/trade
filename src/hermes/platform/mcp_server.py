"""
platform/mcp_server.py — MCP Server

将交易系统能力通过 MCP 暴露给 Hermes Agent。
使用 mcp Python SDK，stdio transport。

所有 tool 直接调用 V2 service，不依赖 V1 scripts/。

启动方式：
  python -m hermes.platform.mcp_server
"""

from __future__ import annotations

import asyncio
import functools
import json
import logging
import traceback
from datetime import date, datetime, timezone
from typing import Optional

from mcp.server.fastmcp import FastMCP

from hermes.platform.db import connect, init_db
from hermes.platform.events import EventStore
from hermes.platform.config import ConfigRegistry
from hermes.platform.runs import RunJournal
from hermes.execution.service import ExecutionService
from hermes.market.service import MarketService
from hermes.market.adapters import (
    AkShareMarketAdapter,
    AkShareFinancialAdapter,
    AkShareFlowAdapter,
    MXMarketAdapter,
    MXSentimentAdapter,
    MXScreenerAdapter,
)
from hermes.market.store import MarketStore
from hermes.reporting.projectors import ProjectionUpdater
from hermes.reporting.reports import ReportGenerator
from hermes.reporting.obsidian import ObsidianProjector
from hermes.risk.service import RiskService
from hermes.risk.sizing import calc_position_size
from hermes.strategy.models import ScoringWeights, MarketSignal, MarketState
from hermes.strategy.scorer import Scorer
from hermes.strategy.decider import Decider
from hermes.strategy.service import StrategyService
from hermes.strategy.timer import compute_market_signal

_logger = logging.getLogger(__name__)

mcp = FastMCP("hermes-trade", instructions="Hermes 量化交易系统 — 评分/风控/持仓/报告")

# ---------------------------------------------------------------------------
# 全局 services（lazy init）
# ---------------------------------------------------------------------------

_conn = None
_event_store: Optional[EventStore] = None
_run_journal: Optional[RunJournal] = None
_exec_svc: Optional[ExecutionService] = None
_report_gen: Optional[ReportGenerator] = None
_market_svc: Optional[MarketService] = None
_strategy_svc: Optional[StrategyService] = None
_config_snapshot = None


def _build_trade_hooks(event_store, conn):
    """Build trade logger hooks if vault is configured."""
    hooks = []
    try:
        from hermes.execution.trade_logger import TradeLogger
        vault_path = _resolve_vault()
        hooks.append(TradeLogger(event_store, conn, vault_path))
    except Exception:
        pass
    return hooks


def _resolve_vault() -> Optional[str]:
    """Resolve vault path from config."""
    try:
        import yaml
        from pathlib import Path
        paths_file = Path(__file__).parent.parent.parent.parent / "config" / "paths.yaml"
        if paths_file.exists():
            with open(paths_file) as f:
                paths = yaml.safe_load(f) or {}
            vp = paths.get("vault_path")
            if vp:
                p = Path(vp)
                if not p.is_absolute():
                    p = Path(__file__).parent.parent.parent.parent / vp
                return str(p)
    except Exception:
        pass
    return None


def _init():
    """Lazy init all services."""
    global _conn, _event_store, _run_journal, _exec_svc, _report_gen
    global _market_svc, _strategy_svc, _config_snapshot

    if _conn is not None:
        return

    init_db()
    _conn = connect()
    _event_store = EventStore(_conn)
    _run_journal = RunJournal(_conn)
    _exec_svc = ExecutionService(_event_store, _conn, on_trade=_build_trade_hooks(_event_store, _conn))
    _report_gen = ReportGenerator(_event_store, _conn)

    store = MarketStore(_conn)
    _market_svc = MarketService(
        market_providers=[MXMarketAdapter(), AkShareMarketAdapter()],
        financial_providers=[AkShareFinancialAdapter()],
        flow_providers=[AkShareFlowAdapter()],
        sentiment_providers=[MXSentimentAdapter()],
        store=store,
    )

    # Load config
    try:
        registry = ConfigRegistry()
        _config_snapshot = registry.freeze(_conn)
        cfg = _config_snapshot.data.get("strategy", {})
    except Exception:
        cfg = {}
        _config_snapshot = None

    weights_cfg = cfg.get("scoring", {}).get("weights", {})
    scorer = Scorer(
        weights=ScoringWeights(
            technical=weights_cfg.get("technical", 3),
            fundamental=weights_cfg.get("fundamental", 2),
            flow=weights_cfg.get("flow", 2),
            sentiment=weights_cfg.get("sentiment", 3),
        ),
        veto_rules=cfg.get("scoring", {}).get("veto", []),
        entry_cfg=cfg.get("entry_signal", {}),
    )
    thresholds = cfg.get("scoring", {}).get("thresholds", {})
    pos_cfg = cfg.get("risk", {}).get("position", {})
    decider = Decider(
        buy_threshold=thresholds.get("buy", 6.5),
        watch_threshold=thresholds.get("watch", 5.0),
        reject_threshold=thresholds.get("reject", 4.0),
        single_max_pct=pos_cfg.get("single_max", 0.20),
        total_max_pct=pos_cfg.get("total_max", 0.60),
        weekly_max=pos_cfg.get("weekly_max", 2),
    )
    _strategy_svc = StrategyService(scorer, decider, _event_store)


def _safe(fn):
    """Decorator: init + catch exceptions."""
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            _init()
            return fn(*args, **kwargs)
        except Exception as e:
            _logger.error(f"Tool error: {e}\n{traceback.format_exc()}")
            return json.dumps({"error": str(e)}, ensure_ascii=False)
    return wrapper


# ---------------------------------------------------------------------------
# Tools — 大盘 & 评分
# ---------------------------------------------------------------------------

@mcp.tool()
@_safe
def trade_market_signal() -> str:
    """获取大盘择时信号（GREEN/YELLOW/RED/CLEAR）和仓位系数。"""
    state = asyncio.run(_market_svc.collect_market_state())
    return json.dumps({
        "signal": state.signal.value,
        "multiplier": state.multiplier,
        "detail": state.detail,
    }, ensure_ascii=False, default=str)


@mcp.tool()
@_safe
def trade_score_stock(code: str) -> str:
    """对单只股票进行四维评分。"""
    config_version = _config_snapshot.version if _config_snapshot else "unknown"
    run_id = f"score_{code}_{datetime.now().strftime('%H%M%S')}"

    snapshot = asyncio.run(_market_svc.collect_snapshot(code, run_id=run_id))
    result = _strategy_svc.score_single(snapshot, run_id, config_version)
    return json.dumps(result.to_dict(), ensure_ascii=False, default=str)


@mcp.tool()
@_safe
def trade_score_batch(codes: str = "") -> str:
    """批量评分。codes 为逗号分隔的股票代码，留空则评核心池。"""
    config_version = _config_snapshot.version if _config_snapshot else "unknown"
    run_id = f"batch_{datetime.now().strftime('%H%M%S')}"

    if codes.strip():
        stock_list = [{"code": c.strip(), "name": ""} for c in codes.split(",") if c.strip()]
    else:
        rows = _conn.execute(
            "SELECT code, name FROM projection_candidate_pool WHERE pool_tier = 'core' ORDER BY score DESC"
        ).fetchall()
        if not rows:
            return json.dumps({"error": "核心池为空，请指定 codes"}, ensure_ascii=False)
        stock_list = [{"code": r["code"], "name": r["name"] or ""} for r in rows]

    snapshots = asyncio.run(_market_svc.collect_batch(stock_list, run_id))
    market_state = asyncio.run(_market_svc.collect_market_state(run_id))
    decisions = _strategy_svc.evaluate(snapshots, market_state, run_id, config_version)

    # Collect results from event_log
    events = _event_store.query(event_type="score.calculated")
    run_scores = [e["payload"] for e in events if e.get("metadata", {}).get("run_id") == run_id]
    run_scores.sort(key=lambda x: x.get("total_score", 0), reverse=True)
    return json.dumps(run_scores, ensure_ascii=False, default=str)


# ---------------------------------------------------------------------------
# Tools — 持仓 & 风控
# ---------------------------------------------------------------------------

@mcp.tool()
@_safe
def trade_portfolio() -> str:
    """查看当前持仓概览。"""
    return json.dumps(_exec_svc.get_portfolio(), ensure_ascii=False, default=str)


@mcp.tool()
@_safe
def trade_pool_status() -> str:
    """查看核心池和观察池状态。"""
    rows = _conn.execute(
        "SELECT * FROM projection_candidate_pool ORDER BY pool_tier, score DESC"
    ).fetchall()
    result = {"core_pool": [], "watch_pool": []}
    for r in rows:
        item = dict(r)
        tier = item.pop("pool_tier", "watch")
        result["core_pool" if tier == "core" else "watch_pool"].append(item)
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool()
@_safe
def trade_check_risk(code: str) -> str:
    """检查单只持仓的风控状态。"""
    from hermes.risk.rules import check_exit_signals, get_risk_params
    from hermes.strategy.models import Style

    pos = _exec_svc.get_position(code)
    if not pos:
        return json.dumps({"error": f"未持有 {code}"}, ensure_ascii=False)

    style = Style(pos.style) if pos.style in ("slow_bull", "momentum") else Style.UNKNOWN
    params = get_risk_params(style)
    today = date.today()

    try:
        entry_date = datetime.strptime(pos.entry_date, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        entry_date = today

    signals = check_exit_signals(
        code=code,
        avg_cost=pos.avg_cost,
        current_price=pos.current_price,
        entry_date=entry_date,
        today=today,
        highest_since_entry=pos.highest_since_entry_cents / 100 if pos.highest_since_entry_cents else pos.avg_cost,
        entry_day_low=pos.entry_day_low_cents / 100 if pos.entry_day_low_cents else pos.avg_cost,
        params=params,
    )
    return json.dumps([{
        "signal_type": s.signal_type,
        "trigger_price": s.trigger_price,
        "current_price": s.current_price,
        "description": s.description,
        "urgency": s.urgency,
    } for s in signals], ensure_ascii=False)


@mcp.tool()
@_safe
def trade_check_portfolio_risk() -> str:
    """检查组合级风控。"""
    from hermes.risk.rules import check_portfolio_risk
    breaches = check_portfolio_risk(
        daily_pnl_pct=0.0, consecutive_loss_days=0,
        max_single_exposure_pct=0.0, max_sector_exposure_pct=0.0,
        limits={"daily_loss_limit_pct": 0.03, "consecutive_loss_days_limit": 2},
    )
    return json.dumps([{
        "rule": b.rule, "current_value": b.current_value,
        "limit_value": b.limit_value, "description": b.description,
    } for b in breaches], ensure_ascii=False)


@mcp.tool()
@_safe
def trade_calc_position(code: str, score: float, price: float) -> str:
    """计算建议仓位。"""
    cfg = _config_snapshot.data.get("strategy", {}) if _config_snapshot else {}
    capital = cfg.get("capital", 500000)
    ps = calc_position_size(total_capital=capital, current_exposure_pct=0.0, price=price, market_multiplier=1.0)
    return json.dumps({
        "code": code, "score": score, "price": price,
        "shares": ps.shares, "amount": ps.amount, "pct": ps.pct,
    }, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Tools — 历史 & 报告
# ---------------------------------------------------------------------------

@mcp.tool()
@_safe
def trade_score_history(code: str, days: int = 7) -> str:
    """查看历史评分记录。"""
    events = _event_store.query(stream=f"strategy:{code}", event_type="score.calculated")
    recent = events[-days:] if len(events) > days else events
    results = [{
        "date": ev.get("occurred_at", "")[:10],
        "total_score": ev["payload"].get("total_score", ev["payload"].get("total", 0)),
        "style": ev["payload"].get("style", ""),
        "veto": ev["payload"].get("veto_triggered", False),
    } for ev in recent]
    return json.dumps({"code": code, "history": results}, ensure_ascii=False)


@mcp.tool()
@_safe
def trade_trade_events(days: int = 7) -> str:
    """查看最近的交易记录。"""
    return _report_gen.generate_trade_history(days)


@mcp.tool()
@_safe
def trade_screener(query: str = "") -> str:
    """选股筛选。"""
    adapter = MXScreenerAdapter()
    cfg = _config_snapshot.data.get("strategy", {}) if _config_snapshot else {}
    q = query.strip() or cfg.get("screening", {}).get("mx_query", "")
    if not q:
        return json.dumps({"error": "请提供筛选条件"}, ensure_ascii=False)
    results = asyncio.run(adapter.search_stocks(q))
    return json.dumps(results, ensure_ascii=False, default=str)


@mcp.tool()
@_safe
def trade_backtest(start: str, end: str, preset: str = "保守验证C") -> str:
    """运行策略回测。"""
    return json.dumps({"error": "回测功能待 V2 回测引擎实现", "start": start, "end": end, "preset": preset}, ensure_ascii=False)


@mcp.tool()
@_safe
def trade_run_pipeline(pipeline_type: str) -> str:
    """运行指定 pipeline（完整流程，带幂等检查）。"""
    if _run_journal.is_completed_today(pipeline_type):
        return json.dumps({"status": "skipped", "reason": f"{pipeline_type} 今日已完成"}, ensure_ascii=False)

    config_version = _config_snapshot.version if _config_snapshot else "unknown"
    run_id = _run_journal.start_run(pipeline_type, config_version)

    try:
        # 构建 pipeline context（复用已初始化的 services）
        from hermes.pipeline.context import PipelineContext
        ctx = PipelineContext(
            conn=_conn, event_store=_event_store, run_journal=_run_journal,
            config_snapshot=_config_snapshot, market_svc=_market_svc,
            strategy_svc=_strategy_svc, risk_svc=RiskService(_event_store),
            exec_svc=_exec_svc, projector=ProjectionUpdater(_event_store, _conn),
            reporter=_report_gen,
            obsidian=ObsidianProjector(_event_store, _conn, _resolve_vault()),
        )

        if pipeline_type == "morning":
            from hermes.pipeline.morning import run
        elif pipeline_type == "scoring":
            from hermes.pipeline.scoring import run
        elif pipeline_type == "evening":
            from hermes.pipeline.evening import run
        elif pipeline_type == "weekly":
            from hermes.pipeline.weekly import run
        else:
            _run_journal.fail_run(run_id, f"Unknown pipeline: {pipeline_type}")
            return json.dumps({"error": f"Unknown pipeline: {pipeline_type}"}, ensure_ascii=False)

        result = run(ctx, run_id)
        _run_journal.complete_run(run_id, artifacts={"result": "ok"})
        return json.dumps({"status": "completed", "run_id": run_id, "pipeline": pipeline_type, **{k: v for k, v in result.items() if k != "discord_embed"}}, ensure_ascii=False, default=str)

    except Exception as e:
        _run_journal.fail_run(run_id, str(e))
        return json.dumps({"status": "failed", "run_id": run_id, "error": str(e)}, ensure_ascii=False)


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------

def main():
    logging.basicConfig(level=logging.INFO)
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
