"""Config registry CLI commands."""

from __future__ import annotations

import typer

from astock_trading.platform.config import ConfigRegistry
from astock_trading.platform.db import connect


config_app = typer.Typer(name="config", help="配置管理")


@config_app.command("freeze")
def config_freeze(
    profile: str = typer.Option("default", help="配置 profile"),
):
    """冻结当前配置为新版本"""
    conn = connect()
    try:
        registry = ConfigRegistry(profile=profile)
        snapshot = registry.freeze(conn)
        typer.echo(f"Config frozen: version={snapshot.version} hash={snapshot.hash}")
    finally:
        conn.close()


@config_app.command("history")
def config_history(
    limit: int = typer.Option(10, help="显示条数"),
):
    """查看配置版本历史"""
    conn = connect()
    try:
        registry = ConfigRegistry()
        versions = registry.list_versions(conn, limit=limit)
        for v in versions:
            activated = v.get("activated_at") or "未使用"
            typer.echo(f"  {v['config_version']}  hash={v['config_hash']}  activated={activated}")
    finally:
        conn.close()
