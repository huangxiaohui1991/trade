"""Paper trading CLI commands."""

from __future__ import annotations

import asyncio

import typer

from astock_trading.platform.cli.common import json_or_text


paper_app = typer.Typer(name="paper", help="模拟盘")


@paper_app.command("status")
def paper_status(
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """查询模拟盘持仓和资金。"""
    from astock_trading.pipeline.paper_account import PaperAccount

    paper = PaperAccount()
    positions = paper.get_positions()
    balance = paper.get_balance()
    payload = {
        "positions": [p.__dict__ for p in positions],
        "balance": balance.__dict__,
    }
    json_or_text(payload, as_json)


@paper_app.command("orders")
def paper_orders(
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """查询模拟盘委托。"""
    from astock_trading.pipeline.paper_account import _mx_call

    result = asyncio.run(_mx_call(lambda c: c.mock_orders()))
    json_or_text(result, as_json)


@paper_app.command("buy")
def paper_buy(
    code: str = typer.Argument(..., help="股票代码"),
    shares: int = typer.Argument(..., help="股数，必须为 100 的整数倍"),
    price: float = typer.Option(0, "--price", help="限价；0 表示市价"),
    yes: bool = typer.Option(False, "--yes", "-y", help="确认下单"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """模拟盘买入。"""
    from astock_trading.pipeline.paper_account import PaperAccount

    if not yes:
        raise typer.BadParameter("paper buy requires --yes")
    result = PaperAccount().buy(code, shares, price)
    json_or_text(result.__dict__, as_json)


@paper_app.command("sell")
def paper_sell(
    code: str = typer.Argument(..., help="股票代码"),
    shares: int = typer.Argument(..., help="股数，必须为 100 的整数倍"),
    price: float = typer.Option(0, "--price", help="限价；0 表示市价"),
    yes: bool = typer.Option(False, "--yes", "-y", help="确认下单"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """模拟盘卖出。"""
    from astock_trading.pipeline.paper_account import PaperAccount

    if not yes:
        raise typer.BadParameter("paper sell requires --yes")
    result = PaperAccount().sell(code, shares, price)
    json_or_text(result.__dict__, as_json)


@paper_app.command("cancel")
def paper_cancel(
    order_id: str = typer.Option("", "--order-id", help="委托 ID；空则撤全部"),
    yes: bool = typer.Option(False, "--yes", "-y", help="确认撤单"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """模拟盘撤单。"""
    from astock_trading.pipeline.paper_account import _mx_call

    if not yes:
        raise typer.BadParameter("paper cancel requires --yes")
    cancel_all = not order_id.strip()
    result = asyncio.run(_mx_call(lambda c: c.mock_cancel(order_id or None, cancel_all)))
    json_or_text(result, as_json)
