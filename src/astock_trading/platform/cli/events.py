"""Event log CLI commands."""

from __future__ import annotations

from typing import Optional

import typer

from astock_trading.platform.cli.common import json_or_text
from astock_trading.platform.db import connect
from astock_trading.platform.events import EventStore


events_app = typer.Typer(name="events", help="事件查询")


@events_app.command("query")
def events_query(
    event_type: Optional[str] = typer.Option(None, "--type", help="事件类型"),
    stream: Optional[str] = typer.Option(None, help="stream 标识"),
    since: Optional[str] = typer.Option(None, help="起始时间 (ISO)"),
    limit: int = typer.Option(50, help="最大条数"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """查询事件"""
    conn = connect()
    try:
        store = EventStore(conn)
        events = store.query(stream=stream, event_type=event_type, since=since, limit=limit)
        if as_json:
            json_or_text(events, True)
        else:
            for e in events:
                typer.echo(
                    f"  [{e['occurred_at']}] {e['event_type']}  "
                    f"stream={e['stream']}  v{e['stream_version']}"
                )
    finally:
        conn.close()


@events_app.command("count")
def events_count(
    event_type: Optional[str] = typer.Option(None, "--type", help="事件类型"),
    since: Optional[str] = typer.Option(None, help="起始时间 (ISO)"),
):
    """统计事件数量"""
    conn = connect()
    try:
        store = EventStore(conn)
        n = store.count(event_type=event_type, since=since)
        typer.echo(f"Events: {n}")
    finally:
        conn.close()
