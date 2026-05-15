"""Manual trade confirmation commands."""

from __future__ import annotations

import typer

from astock_trading.platform.cli.common import json_or_text
from astock_trading.platform.db import connect
from astock_trading.platform.events import EventStore


manual_trades_app = typer.Typer(name="manual-trades", help="人工确认单")


def _manual_trade_state(events: list[dict]) -> list[dict]:
    by_stream: dict[str, dict] = {}
    for event in events:
        payload = event.get("payload", {})
        stream = event.get("stream", "")
        current = by_stream.get(stream, {})
        status = payload.get("status")
        if event["event_type"] == "manual_trade.requested":
            current = {
                **payload,
                "stream": stream,
                "requested_event_id": event["event_id"],
                "requested_at": event["occurred_at"],
                "updated_at": event["occurred_at"],
            }
        elif current:
            current.update(
                {
                    "status": status or event["event_type"].removeprefix("manual_trade."),
                    "updated_at": event["occurred_at"],
                    "resolution_event_id": event["event_id"],
                    "resolution": payload,
                }
            )
        if current:
            by_stream[stream] = current
    return sorted(by_stream.values(), key=lambda item: item.get("updated_at", ""), reverse=True)


@manual_trades_app.command("list")
def manual_trades_list(
    status: str = typer.Option("pending", "--status", help="pending / confirmed / rejected / all"),
    limit: int = typer.Option(100, help="最大事件条数"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """列出人工确认单。"""
    conn = connect()
    try:
        store = EventStore(conn)
        states = _manual_trade_state(store.query(stream_type="manual_trade", limit=limit))
        if status != "all":
            states = [item for item in states if item.get("status") == status]
        if as_json:
            json_or_text(states, True)
        else:
            if not states:
                typer.echo("无人工确认单")
            for item in states:
                typer.echo(
                    f"{item.get('status')} {item.get('side')} "
                    f"{item.get('code')} {item.get('name', '')} "
                    f"score={item.get('score', '-')}"
                )
    finally:
        conn.close()


@manual_trades_app.command("confirm")
def manual_trades_confirm(
    code: str = typer.Argument(..., help="股票代码"),
    order_id: str = typer.Option("", "--order-id", help="关联的手工成交订单 ID"),
    note: str = typer.Option("", "--note", help="确认备注"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """标记人工确认单为已确认。"""
    conn = connect()
    try:
        store = EventStore(conn)
        event_id = store.append(
            stream=f"manual_trade:{code}",
            stream_type="manual_trade",
            event_type="manual_trade.confirmed",
            payload={"status": "confirmed", "code": code, "order_id": order_id, "note": note},
            metadata={"execution": "manual"},
        )
        json_or_text({"status": "confirmed", "event_id": event_id, "code": code}, as_json)
    finally:
        conn.close()


@manual_trades_app.command("reject")
def manual_trades_reject(
    code: str = typer.Argument(..., help="股票代码"),
    reason: str = typer.Option("", "--reason", help="拒绝原因"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """标记人工确认单为已拒绝。"""
    conn = connect()
    try:
        store = EventStore(conn)
        event_id = store.append(
            stream=f"manual_trade:{code}",
            stream_type="manual_trade",
            event_type="manual_trade.rejected",
            payload={"status": "rejected", "code": code, "reason": reason},
            metadata={"execution": "manual"},
        )
        json_or_text({"status": "rejected", "event_id": event_id, "code": code}, as_json)
    finally:
        conn.close()
