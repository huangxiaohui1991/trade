"""风控和仓位计算 CLI 命令。"""

from __future__ import annotations

from datetime import datetime

import typer

from astock_trading.pipeline.adaptive_risk import run_adaptive_risk
from astock_trading.pipeline.context import build_context
from astock_trading.platform.cli.common import json_or_text
from astock_trading.platform.config import ConfigRegistry
from astock_trading.platform.db import connect, init_db
from astock_trading.platform.events import EventStore
from astock_trading.platform.time import local_today
from astock_trading.risk.rules import check_exit_signals, check_portfolio_risk, get_risk_params
from astock_trading.risk.sizing import calc_position_size
from astock_trading.strategy.models import Style


risk_app = typer.Typer(name="risk", help="风控检查和仓位计算")


def _strategy_config() -> dict:
    data, _errors = ConfigRegistry().load_and_validate()
    return data.get("strategy", {})


def _position_limits(strategy: dict) -> dict:
    return strategy.get("risk", {}).get("position", {})


def _risk_limits(strategy: dict) -> dict:
    return strategy.get("risk", {}).get("portfolio", strategy.get("risk", {}))


def _position_style(style: str) -> Style:
    if style == Style.SLOW_BULL.value:
        return Style.SLOW_BULL
    if style == Style.MOMENTUM.value:
        return Style.MOMENTUM
    return Style.UNKNOWN


def _risk_signal_payload(signal) -> dict:
    return {
        "signal_type": signal.signal_type,
        "trigger_price": signal.trigger_price,
        "current_price": signal.current_price,
        "description": signal.description,
        "urgency": signal.urgency,
    }


@risk_app.command("position")
def risk_position(
    code: str = typer.Argument(..., help="股票代码"),
    score: float = typer.Argument(..., help="评分，用于记录本次仓位建议的依据"),
    price: float = typer.Argument(..., help="当前价格"),
    capital: float | None = typer.Option(None, "--capital", help="总资金；默认读取 strategy.capital"),
    current_exposure_pct: float = typer.Option(0.0, "--current-exposure-pct", help="当前总仓位占比"),
    market_multiplier: float = typer.Option(1.0, "--market-multiplier", help="大盘仓位系数"),
    single_max_pct: float | None = typer.Option(None, "--single-max-pct", help="单股仓位上限"),
    total_max_pct: float | None = typer.Option(None, "--total-max-pct", help="总仓位上限"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """计算建议仓位；只读，不写入数据库。"""
    strategy = _strategy_config()
    position_cfg = _position_limits(strategy)
    total_capital = capital if capital is not None else float(strategy.get("capital", 500000))
    single_max = single_max_pct if single_max_pct is not None else float(position_cfg.get("single_max", 0.20))
    total_max = total_max_pct if total_max_pct is not None else float(position_cfg.get("total_max", 0.60))

    size = calc_position_size(
        total_capital=total_capital,
        current_exposure_pct=current_exposure_pct,
        price=price,
        market_multiplier=market_multiplier,
        single_max_pct=single_max,
        total_max_pct=total_max,
    )
    payload = {
        "code": code,
        "score": score,
        "price": price,
        "capital": total_capital,
        "current_exposure_pct": current_exposure_pct,
        "market_multiplier": market_multiplier,
        "single_max_pct": single_max,
        "total_max_pct": total_max,
        "shares": size.shares,
        "amount": size.amount,
        "pct": size.pct,
    }
    json_or_text(payload, as_json)


@risk_app.command("trial-guard")
def risk_trial_guard(
    capital: float | None = typer.Option(None, "--capital", help="总资金；默认读取 strategy.capital"),
    amount: float | None = typer.Option(None, "--amount", help="拟执行单笔金额，用于检查是否超过试运行上限"),
    trial_ratio: float | None = typer.Option(None, "--trial-ratio", help="试运行比例；默认正式单票上限的一半"),
    single_max_pct: float | None = typer.Option(None, "--single-max-pct", help="正式单票仓位上限"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """审计首轮实盘试运行护栏；只读，不执行交易。"""
    strategy = _strategy_config()
    position_cfg = _position_limits(strategy)
    total_capital = capital if capital is not None else float(strategy.get("capital", 500000))
    single_max = single_max_pct if single_max_pct is not None else float(position_cfg.get("single_max", 0.20))
    ratio = trial_ratio if trial_ratio is not None else float(position_cfg.get("trial_single_max_ratio", 0.50))
    cap_pct = round(single_max * ratio, 4)
    cap_amount = round(total_capital * cap_pct, 2)
    checked_order = None
    status = "ok"
    if amount is not None:
        within_cap = amount <= cap_amount
        checked_order = {
            "amount": amount,
            "within_cap": within_cap,
            "excess_amount": round(max(amount - cap_amount, 0), 2),
        }
        if not within_cap:
            status = "breached"

    payload = {
        "status": status,
        "manual_confirmation_required": True,
        "real_broker_integration": "disabled",
        "real_order_auto_execution_allowed": False,
        "trial_position_cap": {
            "capital": total_capital,
            "formal_single_max_pct": single_max,
            "trial_ratio": ratio,
            "cap_pct": cap_pct,
            "cap_amount": cap_amount,
        },
        "checked_order": checked_order,
        "instructions": [
            "系统只生成买入意向和记录人工成交，不直连券商实盘下单。",
            "首轮实盘单笔金额应按试运行上限人工确认；超限时先降低股数或放弃执行。",
        ],
    }
    json_or_text(payload, as_json)


@risk_app.command("adaptive")
def risk_adaptive(
    lookback_days: int = typer.Option(20, "--lookback-days", help="自适应风控证据回看天数"),
    min_market_bars: int = typer.Option(10, "--min-market-bars", help="波动率建议所需的最少 K 线样本"),
    record: bool = typer.Option(False, "--record/--no-record", help="是否记录 risk.adaptive_suggestion.proposed 事件"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """P6-1 自适应风控建议；只读，不自动改配置或下单。"""
    if lookback_days < 1:
        raise typer.BadParameter("--lookback-days must be >= 1")
    if min_market_bars < 1:
        raise typer.BadParameter("--min-market-bars must be >= 1")

    ctx = build_context()
    try:
        payload = run_adaptive_risk(
            ctx.conn,
            lookback_days=lookback_days,
            min_market_bars=min_market_bars,
            record=record,
            config_version=ctx.config_version,
        )
        if as_json:
            json_or_text(payload, True)
            return
        typer.echo(payload["report_markdown"])
    finally:
        ctx.conn.close()


@risk_app.command("check")
def risk_check(
    code: str = typer.Argument(..., help="股票代码"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """检查单只本地持仓的离场风控信号。"""
    init_db()
    conn = connect()
    try:
        from astock_trading.execution.service import ExecutionService

        svc = ExecutionService(EventStore(conn), conn)
        pos = svc.get_position(code)
        if not pos:
            json_or_text({"status": "not_held", "code": code, "signals": []}, as_json)
            return

        style = _position_style(pos.style)
        risk_cfg = _strategy_config().get("risk", {})
        params = get_risk_params(style, risk_cfg)
        today = local_today()
        try:
            entry_date = datetime.strptime(pos.entry_date, "%Y-%m-%d").date()
        except (TypeError, ValueError):
            entry_date = today

        signals = check_exit_signals(
            code=code,
            avg_cost=pos.avg_cost,
            current_price=pos.current_price or pos.avg_cost,
            entry_date=entry_date,
            today=today,
            highest_since_entry=(
                pos.highest_since_entry_cents / 100 if pos.highest_since_entry_cents else pos.avg_cost
            ),
            entry_day_low=pos.entry_day_low_cents / 100 if pos.entry_day_low_cents else pos.avg_cost,
            params=params,
        )
        payload = {
            "status": "ok",
            "code": code,
            "position": pos.to_dict(),
            "signals": [_risk_signal_payload(signal) for signal in signals],
        }
        json_or_text(payload, as_json)
    finally:
        conn.close()


@risk_app.command("portfolio")
def risk_portfolio(
    daily_pnl_pct: float = typer.Option(0.0, "--daily-pnl-pct", help="单日收益率，用小数表示"),
    consecutive_loss_days: int = typer.Option(0, "--consecutive-loss-days", help="连续亏损天数"),
    max_sector_exposure_pct: float = typer.Option(0.0, "--max-sector-exposure-pct", help="最大行业仓位占比"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """检查组合级风控限制；只读。"""
    init_db()
    conn = connect()
    try:
        from astock_trading.execution.service import ExecutionService

        portfolio = ExecutionService(EventStore(conn), conn).get_portfolio()
        positions = portfolio.get("positions", [])
        total_market = portfolio.get("total_market_cents", 0) or 0
        if total_market > 0:
            max_single_exposure_pct = max(
                ((item.get("current_price_cents") or item.get("avg_cost_cents") or 0) * item.get("shares", 0))
                / total_market
                for item in positions
            )
        else:
            max_single_exposure_pct = 0.0

        limits = _risk_limits(_strategy_config())
        breaches = check_portfolio_risk(
            daily_pnl_pct=daily_pnl_pct,
            consecutive_loss_days=consecutive_loss_days,
            max_single_exposure_pct=max_single_exposure_pct,
            max_sector_exposure_pct=max_sector_exposure_pct,
            limits=limits,
        )
        payload = {
            "status": "breached" if breaches else "ok",
            "inputs": {
                "daily_pnl_pct": daily_pnl_pct,
                "consecutive_loss_days": consecutive_loss_days,
                "max_single_exposure_pct": max_single_exposure_pct,
                "max_sector_exposure_pct": max_sector_exposure_pct,
            },
            "breaches": [
                {
                    "rule": breach.rule,
                    "current_value": breach.current_value,
                    "limit_value": breach.limit_value,
                    "description": breach.description,
                }
                for breach in breaches
            ],
        }
        json_or_text(payload, as_json)
    finally:
        conn.close()
