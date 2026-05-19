"""P5 参数校准 CLI。"""

from __future__ import annotations

import typer

from astock_trading.pipeline.context import build_context
from astock_trading.pipeline.param_calibration import run_calibration
from astock_trading.platform.cli.common import json_or_text


def register_calibration_command(app: typer.Typer) -> None:
    @app.command("calibrate")
    def calibrate(
        min_samples: int = typer.Option(20, "--min-samples", help="输出参数建议所需的最小闭合交易复盘数"),
        window_days: int = typer.Option(365, "--window-days", help="样本回看天数"),
        record: bool = typer.Option(False, "--record/--no-record", help="是否记录 strategy.calibration.proposed 事件"),
        as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
    ) -> None:
        """P5 参数校准：输出参数、权重和选股条件建议，不自动改配置。"""
        if min_samples < 1:
            raise typer.BadParameter("--min-samples must be >= 1")
        if window_days < 1:
            raise typer.BadParameter("--window-days must be >= 1")

        ctx = build_context()
        try:
            payload = run_calibration(
                ctx.conn,
                min_samples=min_samples,
                window_days=window_days,
                record=record,
                config_version=ctx.config_version,
            )
            if as_json:
                json_or_text(payload, True)
                return
            typer.echo(payload["report_markdown"])
        finally:
            ctx.conn.close()
