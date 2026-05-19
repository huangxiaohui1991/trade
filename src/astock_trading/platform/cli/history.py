"""历史信号镜像 CLI。"""

from __future__ import annotations

import typer

from astock_trading.platform.cli.common import json_or_text
from astock_trading.platform.db import connect, init_db
from astock_trading.platform.history_mirror import diagnose_signal_history


history_app = typer.Typer(name="history", help="历史信号镜像")


@history_app.command("signal")
def history_signal(
    snapshot_date: str = typer.Option("", "--date", help="快照日期 YYYY-MM-DD，默认今天"),
    history_group_id: str = typer.Option("", "--history-group-id", help="历史镜像分组 ID"),
    code: str = typer.Option("", "--code", help="只解释某只股票代码"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """按日期/group/code 查看 market / pool / candidates / decision 历史镜像。"""
    init_db()
    conn = connect()
    try:
        payload = diagnose_signal_history(
            conn,
            snapshot_date=snapshot_date or None,
            history_group_id=history_group_id,
            code=code,
        )
        json_or_text(payload, as_json)
    finally:
        conn.close()
