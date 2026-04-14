"""
platform/cli.py — CLI 入口 (typer)

人工调试用。与 MCP Server 共享同一套 service 代码。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer

from hermes.platform.db import connect, init_db, get_schema_version
from hermes.platform.events import EventStore
from hermes.platform.config import ConfigRegistry
from hermes.platform.runs import RunJournal

app = typer.Typer(name="hermes", help="Hermes 交易系统 CLI")
db_app = typer.Typer(name="db", help="数据库管理")
config_app = typer.Typer(name="config", help="配置管理")
runs_app = typer.Typer(name="runs", help="运行记录")
events_app = typer.Typer(name="events", help="事件查询")

app.add_typer(db_app)
app.add_typer(config_app)
app.add_typer(runs_app)
app.add_typer(events_app)


# ── db commands ───────────────────────────────────────────────

@db_app.command("init")
def db_init(
    db_path: Optional[Path] = typer.Option(None, help="数据库路径"),
):
    """初始化数据库（创建所有表）"""
    path = init_db(db_path)
    typer.echo(f"数据库已初始化: {path}")


@db_app.command("status")
def db_status(
    db_path: Optional[Path] = typer.Option(None, help="数据库路径"),
):
    """查看数据库状态"""
    conn = connect(db_path)
    try:
        version = get_schema_version(conn)
        event_count = conn.execute("SELECT COUNT(*) FROM event_log").fetchone()[0]
        run_count = conn.execute("SELECT COUNT(*) FROM run_log").fetchone()[0]
        config_count = conn.execute("SELECT COUNT(*) FROM config_versions").fetchone()[0]
        typer.echo(f"Schema version: {version}")
        typer.echo(f"Events: {event_count}")
        typer.echo(f"Runs: {run_count}")
        typer.echo(f"Config versions: {config_count}")
    finally:
        conn.close()


# ── config commands ───────────────────────────────────────────

@config_app.command("freeze")
def config_freeze(
    profile: str = typer.Option("default", help="配置 profile"),
    db_path: Optional[Path] = typer.Option(None, help="数据库路径"),
):
    """冻结当前配置为新版本"""
    conn = connect(db_path)
    try:
        registry = ConfigRegistry(profile=profile)
        snapshot = registry.freeze(conn)
        typer.echo(f"Config frozen: version={snapshot.version} hash={snapshot.hash}")
    finally:
        conn.close()


@config_app.command("history")
def config_history(
    limit: int = typer.Option(10, help="显示条数"),
    db_path: Optional[Path] = typer.Option(None, help="数据库路径"),
):
    """查看配置版本历史"""
    conn = connect(db_path)
    try:
        registry = ConfigRegistry()
        versions = registry.list_versions(conn, limit=limit)
        for v in versions:
            activated = v.get("activated_at") or "未使用"
            typer.echo(f"  {v['config_version']}  hash={v['config_hash']}  activated={activated}")
    finally:
        conn.close()


# ── runs commands ─────────────────────────────────────────────

@runs_app.command("list")
def runs_list(
    run_type: Optional[str] = typer.Option(None, help="过滤 run_type"),
    status: Optional[str] = typer.Option(None, help="过滤 status"),
    limit: int = typer.Option(20, help="显示条数"),
    db_path: Optional[Path] = typer.Option(None, help="数据库路径"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """查看运行记录"""
    conn = connect(db_path)
    try:
        journal = RunJournal(conn)
        runs = journal.list_runs(run_type=run_type, status=status, limit=limit)
        if as_json:
            typer.echo(json.dumps(runs, ensure_ascii=False, indent=2))
        else:
            for r in runs:
                status_icon = {"completed": "✅", "failed": "❌", "running": "⏳"}.get(
                    r["status"], "?"
                )
                typer.echo(
                    f"  {status_icon} {r['run_id']}  type={r['run_type']}  "
                    f"status={r['status']}  started={r['started_at']}"
                )
    finally:
        conn.close()


@runs_app.command("failed")
def runs_failed(
    days: int = typer.Option(7, help="查看最近 N 天"),
    db_path: Optional[Path] = typer.Option(None, help="数据库路径"),
):
    """查看近期失败的运行"""
    conn = connect(db_path)
    try:
        journal = RunJournal(conn)
        failed = journal.get_failed_runs(days=days)
        if not failed:
            typer.echo("无失败记录 🎉")
        else:
            for r in failed:
                typer.echo(
                    f"  ❌ {r['run_id']}  type={r['run_type']}  "
                    f"error={r.get('error_message', '')[:80]}"
                )
    finally:
        conn.close()


# ── events commands ───────────────────────────────────────────

@events_app.command("query")
def events_query(
    event_type: Optional[str] = typer.Option(None, "--type", help="事件类型"),
    stream: Optional[str] = typer.Option(None, help="stream 标识"),
    since: Optional[str] = typer.Option(None, help="起始时间 (ISO)"),
    limit: int = typer.Option(50, help="最大条数"),
    db_path: Optional[Path] = typer.Option(None, help="数据库路径"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """查询事件"""
    conn = connect(db_path)
    try:
        store = EventStore(conn)
        events = store.query(
            stream=stream, event_type=event_type, since=since, limit=limit
        )
        if as_json:
            typer.echo(json.dumps(events, ensure_ascii=False, indent=2))
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
    db_path: Optional[Path] = typer.Option(None, help="数据库路径"),
):
    """统计事件数量"""
    conn = connect(db_path)
    try:
        store = EventStore(conn)
        n = store.count(event_type=event_type, since=since)
        typer.echo(f"Events: {n}")
    finally:
        conn.close()


def main():
    app()


if __name__ == "__main__":
    main()
