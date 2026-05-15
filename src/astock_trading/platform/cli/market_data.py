"""Market data loading CLI commands."""

from __future__ import annotations

import asyncio
import json

import typer

from astock_trading.platform.db import connect


def register_market_data_commands(app: typer.Typer) -> None:
    @app.command("fetch-history")
    def fetch_history(
        code: str = typer.Argument(..., help="股票代码（支持 600036 / sh.600036 / sz.000001）"),
        period: str = typer.Option("daily", help="周期: daily | weekly | monthly | 5 | 15 | 30 | 60"),
        start_date: str = typer.Option("", help="开始日期 YYYY-MM-DD（空则往前推 count 条）"),
        end_date: str = typer.Option("", help="结束日期 YYYY-MM-DD（空则默认今天）"),
        count: int = typer.Option(500, help="最大条数（start_date 为空时生效）"),
        adjustflag: str = typer.Option("2", help="复权: 2=前复权 1=后复权 3=不复权"),
        as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
    ):
        """通过 baostock 拉取历史 K 线并写入 market_bars 表。"""
        conn = connect()
        try:
            from astock_trading.market.adapters import BaoStockMarketAdapter
            from astock_trading.market.store import MarketStore

            adapter = BaoStockMarketAdapter()
            store = MarketStore(conn)

            if start_date:
                df = asyncio.run(
                    adapter.get_kline(
                        code,
                        period=period,
                        start_date=start_date or None,
                        end_date=end_date or None,
                        adjustflag=adjustflag,
                    )
                )
            else:
                df = asyncio.run(adapter.get_kline(code, period=period, count=count, adjustflag=adjustflag))

            if df is None or df.empty:
                typer.echo(f"获取数据失败: {code}", err=True)
                raise typer.Exit(1)

            rows = store.save_bars(code, df, source="baostock")
            conn.commit()

            result = {
                "status": "ok",
                "code": code,
                "period": period,
                "adjustflag": adjustflag,
                "fetched": len(df),
                "saved": rows,
                "date_range": f"{df['日期'].iloc[0]} ~ {df['日期'].iloc[-1]}",
            }
            if as_json:
                typer.echo(json.dumps(result, ensure_ascii=False, indent=2))
            else:
                typer.echo(f"{code} {period} [{adjustflag}] -> 写入 {rows} 条 ({result['date_range']})")
        finally:
            conn.close()
