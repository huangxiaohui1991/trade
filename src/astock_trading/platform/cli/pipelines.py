"""Pipeline execution CLI commands."""

from __future__ import annotations

import typer

from astock_trading.platform.pipeline_policy import data_source_gate_decision, should_skip_pipeline
from astock_trading.platform.time import is_trading_day, local_today_str


def register_pipeline_commands(app: typer.Typer) -> None:
    @app.command("run-pipeline")
    def run_pipeline(
        pipeline_type: str = typer.Argument(..., help="morning | noon | intraday_monitor | evening | scoring | weekly"),
        ignore_data_source_health: bool = typer.Option(
            False,
            "--ignore-data-source-health",
            help="忽略数据源健康 gate，强制运行依赖行情的 pipeline",
        ),
    ):
        """运行指定 pipeline（完整流程，带幂等检查）"""
        from astock_trading.pipeline.context import build_context

        ctx = build_context()
        try:
            skip_reason = should_skip_pipeline(
                pipeline_type,
                is_trading_day=is_trading_day(),
                is_completed_today=ctx.run_journal.is_completed_today(pipeline_type),
            )
            if skip_reason == "non_trading_day":
                typer.echo(f"今日（{local_today_str()}）非交易日，{pipeline_type} 跳过")
                return
            if skip_reason == "completed_today":
                typer.echo(f"{pipeline_type} 今日已完成，跳过")
                return

            run_id = ctx.run_journal.start_run(pipeline_type, ctx.config_version)
            typer.echo(f"{pipeline_type} 开始 (run_id={run_id})")

            try:
                data_health = None
                if not ignore_data_source_health:
                    from astock_trading.market.health import evaluate_data_source_health

                    data_health = evaluate_data_source_health(ctx.conn)
                    gate = data_source_gate_decision(pipeline_type, data_health)
                    if gate == "failed":
                        missing = ",".join(data_health.get("required_missing", []))
                        message = f"核心数据源不可用，{pipeline_type} 跳过: {missing}"
                        ctx.run_journal.fail_run(run_id, message, artifacts={"data_sources": data_health})
                        typer.echo(message, err=True)
                        raise typer.Exit(1)
                    if gate == "warning":
                        missing = ",".join(data_health.get("optional_missing", []))
                        typer.echo(f"辅助数据源降级，{pipeline_type} 继续运行: {missing}")

                if pipeline_type == "morning":
                    from astock_trading.pipeline.morning import run

                    result = run(ctx, run_id)
                    typer.echo(f"  大盘={result['signal']} 持仓={result['positions']} 风控={len(result['risk_alerts'])}条")
                elif pipeline_type == "noon":
                    from astock_trading.pipeline.noon import run

                    result = run(ctx, run_id)
                    typer.echo(f"  大盘={result['signal']} 持仓={result['positions']} 风控={len(result['alerts'])}条")
                elif pipeline_type == "intraday_monitor":
                    from astock_trading.pipeline.intraday_monitor import run

                    result = run(ctx, run_id)
                    typer.echo(
                        f"  持仓={result['positions']} "
                        f"新告警={len(result['alerts'])}条 去重={result['deduped']}条"
                    )
                elif pipeline_type == "scoring":
                    from astock_trading.pipeline.scoring import run

                    result = run(ctx, run_id)
                    typer.echo(f"  评分 {result['scored']} 只股票")
                elif pipeline_type == "evening":
                    from astock_trading.pipeline.evening import run

                    result = run(ctx, run_id)
                    typer.echo(f"  大盘={result['signal']} 持仓={result['positions']} 风控={len(result['risk_alerts'])}条")
                elif pipeline_type == "weekly":
                    from astock_trading.pipeline.weekly import run

                    result = run(ctx, run_id)
                    typer.echo(f"  {result['buy_count']}买 {result['sell_count']}卖 胜率{result['win_rate']:.0%}")
                elif pipeline_type == "sentiment":
                    from astock_trading.pipeline.sentiment import run as sentiment_run

                    result = sentiment_run(ctx, run_id)
                    typer.echo(f"  监控{result['monitored']}只 告警{len(result['alerts'])}条")
                elif pipeline_type == "auto_trade":
                    from astock_trading.pipeline.auto_trade import run as auto_trade_run

                    result = auto_trade_run(ctx, run_id)
                    if not result.get("enabled"):
                        typer.echo("auto_trade 未启用")
                    else:
                        mode = "[DRY]" if result.get("dry_run") else ""
                        typer.echo(f"  {mode} 买入{len(result['buys'])}笔 卖出{len(result['sells'])}笔")
                else:
                    ctx.run_journal.fail_run(run_id, f"Unknown pipeline: {pipeline_type}")
                    typer.echo(f"Unknown pipeline: {pipeline_type}", err=True)
                    raise typer.Exit(1)

                artifacts = {"result": "ok"}
                if data_health is not None:
                    artifacts["data_sources"] = data_health
                ctx.run_journal.complete_run(run_id, artifacts=artifacts)
                typer.echo(f"{pipeline_type} 完成")
            except typer.Exit:
                raise
            except Exception as e:
                ctx.run_journal.fail_run(run_id, str(e))
                typer.echo(f"{pipeline_type} 失败: {e}", err=True)
                raise typer.Exit(1)
        finally:
            ctx.conn.close()

    @app.command("refresh-positions")
    def refresh_positions_cmd():
        """刷新持仓实时价格并写 DB（自动跳过缓存未过期的）。"""
        from astock_trading.pipeline.context import build_context

        ctx = build_context()
        try:
            from astock_trading.pipeline.helpers import refresh_position_prices

            prices = refresh_position_prices(ctx)
            if not prices:
                typer.echo("无持仓")
            else:
                typer.echo(f"已刷新 {len(prices)} 只持仓:")
                for code, price in prices.items():
                    typer.echo(f"  {code}  ¥{price:.2f}")
        finally:
            ctx.conn.close()
