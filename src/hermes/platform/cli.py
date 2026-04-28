"""
platform/cli.py — CLI 入口 (typer)

人工调试用。与 MCP Server 共享同一套 service 代码。
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Optional

import typer

from hermes.platform.db import connect, init_db, get_schema_version
from hermes.platform.events import EventStore
from hermes.platform.config import ConfigRegistry
from hermes.platform.runs import RunJournal
from hermes.platform.time import MARKET_TZ

app = typer.Typer(name="trade", help="Hermes 交易系统 CLI")
db_app = typer.Typer(name="db", help="数据库管理")
config_app = typer.Typer(name="config", help="配置管理")
runs_app = typer.Typer(name="runs", help="运行记录")
events_app = typer.Typer(name="events", help="事件查询")

app.add_typer(db_app)
app.add_typer(config_app)
app.add_typer(runs_app)
app.add_typer(events_app)


def _format_metric(value: object, fmt: str, fallback: str = "n/a") -> str:
    if value is None:
        return fallback
    try:
        return format(value, fmt)
    except (TypeError, ValueError):
        return fallback


def _parse_int_csv(value: str) -> tuple[int, ...]:
    parsed = tuple(dict.fromkeys(int(part.strip()) for part in value.split(",") if part.strip()))
    if not parsed:
        raise typer.BadParameter("至少提供一个正整数")
    if any(item <= 0 for item in parsed):
        raise typer.BadParameter("只支持正整数")
    return parsed


def _resolve_vault_path() -> Optional[Path]:
    paths_file = Path(__file__).resolve().parent.parent.parent.parent / "config" / "paths.yaml"
    if not paths_file.exists():
        return None

    try:
        import yaml

        with open(paths_file, encoding="utf-8") as f:
            paths = yaml.safe_load(f) or {}
    except Exception:
        return None

    raw_path = paths.get("vault_path")
    if not raw_path:
        return None

    path = Path(raw_path)
    if not path.is_absolute():
        path = paths_file.parent.parent / path
    return path


@app.command("doctor")
def doctor(
    db_path: Optional[Path] = typer.Option(None, help="数据库路径"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """环境自检：数据库、配置、vault、MCP 依赖。"""
    path = init_db(db_path)
    conn = connect(db_path)
    try:
        version = get_schema_version(conn)
        event_count = conn.execute("SELECT COUNT(*) FROM event_log").fetchone()[0]
        run_count = conn.execute("SELECT COUNT(*) FROM run_log").fetchone()[0]
        config_count = conn.execute("SELECT COUNT(*) FROM config_versions").fetchone()[0]

        registry = ConfigRegistry()
        snapshot = registry.freeze(conn)

        vault_path = _resolve_vault_path()
        result = {
            "status": "ok",
            "db": {
                "path": str(path),
                "schema_version": version,
                "events": event_count,
                "runs": run_count,
                "config_versions": config_count,
            },
            "config": {
                "version": snapshot.version,
                "hash": snapshot.hash,
            },
            "vault": {
                "path": str(vault_path) if vault_path else "",
                "exists": bool(vault_path and vault_path.exists()),
            },
            "mcp": {
                "installed": importlib.util.find_spec("mcp.server.fastmcp") is not None,
            },
            "timezone": str(MARKET_TZ),
        }

        if as_json:
            typer.echo(json.dumps(result, ensure_ascii=False, indent=2))
            return

        typer.echo("Hermes Doctor")
        typer.echo(f"  DB: {result['db']['path']}")
        typer.echo(f"  Schema version: {result['db']['schema_version']}")
        typer.echo(
            f"  Events/Runs/Configs: "
            f"{result['db']['events']}/{result['db']['runs']}/{result['db']['config_versions']}"
        )
        typer.echo(
            f"  Config: {result['config']['version']} "
            f"(hash={result['config']['hash']})"
        )
        typer.echo(
            f"  Vault: {result['vault']['path'] or '未配置'} "
            f"(exists={result['vault']['exists']})"
        )
        typer.echo(f"  MCP installed: {result['mcp']['installed']}")
        typer.echo(f"  Business timezone: {result['timezone']}")
    finally:
        conn.close()


# ── db commands ───────────────────────────────────────────────

@db_app.command("init")
def db_init(
    db_path: Optional[Path] = typer.Option(None, help="数据库路径"),
):
    """初始化数据库（创建所有表）"""
    path = init_db(db_path)
    typer.echo(f"数据库已初始化: {path}")


@db_app.command("migrate")
def db_migrate(
    db_path: Optional[Path] = typer.Option(None, help="数据库路径"),
):
    """运行数据库 migration（创建缺失的表，更新 schema 版本）"""
    path = init_db(db_path)
    conn = connect(db_path)
    try:
        version = get_schema_version(conn)
        typer.echo(f"Migration 完成: schema v{version} @ {path}")
    finally:
        conn.close()


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


# ── history commands ────────────────────────────────────────────

@app.command("fetch-history")
def fetch_history(
    code: str = typer.Argument(..., help="股票代码（支持 600036 / sh.600036 / sz.000001）"),
    period: str = typer.Option("daily", help="周期: daily | weekly | monthly | 5 | 15 | 30 | 60"),
    start_date: str = typer.Option("", help="开始日期 YYYY-MM-DD（空则往前推 count 条）"),
    end_date: str = typer.Option("", help="结束日期 YYYY-MM-DD（空则默认今天）"),
    count: int = typer.Option(500, help="最大条数（start_date 为空时生效）"),
    adjustflag: str = typer.Option("2", help="复权: 2=前复权 1=后复权 3=不复权"),
    db_path: Optional[Path] = typer.Option(None, help="数据库路径"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """通过 baostock 拉取历史 K 线并写入 market_bars 表。"""
    import asyncio
    conn = connect(db_path)
    try:
        from hermes.market.adapters import BaoStockMarketAdapter
        from hermes.market.store import MarketStore

        adapter = BaoStockMarketAdapter()
        store = MarketStore(conn)

        if start_date:
            df = asyncio.run(adapter.get_kline(
                code, period=period,
                start_date=start_date or None, end_date=end_date or None,
                adjustflag=adjustflag,
            ))
        else:
            df = asyncio.run(adapter.get_kline(
                code, period=period, count=count, adjustflag=adjustflag,
            ))

        if df is None or df.empty:
            typer.echo(f"❌ 获取数据失败: {code}", err=True)
            raise typer.Exit(1)

        rows = store.save_bars(code, df, source="baostock")
        conn.commit()

        result = {
            "status": "ok", "code": code, "period": period,
            "adjustflag": adjustflag, "fetched": len(df), "saved": rows,
            "date_range": f"{df['日期'].iloc[0]} ~ {df['日期'].iloc[-1]}",
        }
        if as_json:
            typer.echo(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            typer.echo(f"✅ {code} {period} [{adjustflag}] → 写入 {rows} 条 ({result['date_range']})")
    finally:
        conn.close()


@app.command("backtest")
def run_backtest_cmd(
    codes: str = typer.Argument(..., help="逗号分隔股票代码，如 600036,000001,000002"),
    start: str = typer.Argument(..., help="回测开始日期 YYYY-MM-DD"),
    end: str = typer.Argument(..., help="回测结束日期 YYYY-MM-DD"),
    preset: str = typer.Option("保守验证C", help="策略 preset（对应 strategy.yaml）"),
    initial_cash: float = typer.Option(100000.0, help="初始资金（元）"),
    adjustflag: str = typer.Option("2", help="复权: 2=前复权 1=后复权 3=不复权"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """运行历史回测（生产级四维评分引擎 + baostock 数据）。"""
    from hermes.backtest.engine import run_backtest

    result = run_backtest(
        codes=codes, start=start, end=end,
        preset=preset, initial_cash=initial_cash, adjustflag=adjustflag,
    )

    if "error" in result:
        typer.echo(f"\u274c {result['error']}", err=True)
        raise typer.Exit(1)

    if as_json:
        typer.echo(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        typer.echo(f"📊 回测报告 [{result['preset']}] {start} ~ {end}")
        typer.echo(f"  初始资金: {result['initial_cash']:.0f}  最终: {result['final_value']:.2f}")
        typer.echo(f"  总收益率: {result['total_return_pct']:.2f}%  年化: {result['annual_return_pct']:.2f}%")
        typer.echo(f"  最大回撤: {result['max_drawdown_pct']:.2f}%  胜率: {result['win_rate_pct']:.1f}%")
        typer.echo(f"  夏普比率: {result.get('sharpe_ratio', 0):.2f}")
        typer.echo(f"  交易: {result['total_trades']}笔 买/{result['buy_trades']} 卖/{result['sell_trades']} 胜/{result.get('winning_trades', 0)} 负/{result.get('losing_trades', 0)}")
        typer.echo(f"  持仓中: {result['positions_open']} 只")


@app.command("continuation-validate")
def continuation_validate_cmd(
    codes: str = typer.Argument(..., help="逗号分隔股票代码"),
    start: str = typer.Option(..., help="验证开始日期 YYYY-MM-DD"),
    end: str = typer.Option(..., help="验证结束日期 YYYY-MM-DD"),
    top_n: Optional[int] = typer.Option(None, help="每日保留 Top N（默认读 continuation.scoring.top_n）"),
    db_path: Optional[Path] = typer.Option(None, help="数据库路径"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """运行短线续涨评分验证并输出分层和 Top N 报告。"""
    from hermes.research.continuation_validation import run_continuation_validation

    result = run_continuation_validation(
        codes=[c.strip() for c in codes.split(",") if c.strip()],
        start=start,
        end=end,
        top_n=top_n,
        db_path=db_path,
    )

    if as_json:
        typer.echo(json.dumps(result, ensure_ascii=False, indent=2))
        return

    typer.echo(f"短线续涨验证 {start} ~ {end}")
    typer.echo(f"  Top N: {result['top_n']}")
    typer.echo(f"  Buckets: {len(result['score_bucket_report'])}")
    typer.echo(f"  Execution modes: {len(result['execution_report'])}")
    if result["top_candidates"]:
        typer.echo("  Top candidates:")
        report_rows = result.get("candidate_report", result["top_candidates"])
        for row in report_rows[: min(5, len(report_rows))]:
            scores = row.get("scores", {})
            metrics = row.get("metrics", {})
            forward = row.get("forward_returns", {})
            score_text = _format_metric(row.get("score"), ".1f")
            t1_text = _format_metric(forward.get("t1", row.get("t1_return")), ".2%")
            typer.echo(
                f"    {row['trade_date']} #{row['rank']} {row['code']} "
                f"score={score_text} "
                f"t1={t1_text}"
            )
            typer.echo(
                "      "
                f"S={_format_metric(scores.get('strength', row.get('strength_score')), '.2f', '0.00')} "
                f"C={_format_metric(scores.get('continuity', row.get('continuity_score')), '.2f', '0.00')} "
                f"Q={_format_metric(scores.get('quality', row.get('quality_score')), '.2f', '0.00')} "
                f"F={_format_metric(scores.get('flow', row.get('flow_score')), '.2f', '0.00')} "
                f"St={_format_metric(scores.get('stability', row.get('stability_score')), '.2f', '0.00')} "
                f"P={_format_metric(scores.get('penalty', row.get('overheat_penalty')), '.2f', '0.00')}"
            )
            typer.echo(
                "      "
                f"chg={_format_metric(metrics.get('change_pct'), '.2f')}% "
                f"cnh={_format_metric(metrics.get('close_near_high'), '.2f')} "
                f"mom5={_format_metric(metrics.get('momentum_5d'), '.2f')} "
                f"ret={_format_metric(metrics.get('intraday_retrace'), '.2%')} "
                f"body={_format_metric(metrics.get('body_ratio'), '.2f')} "
                f"rsi={_format_metric(metrics.get('rsi'), '.1f')} "
                f"vr={_format_metric(metrics.get('volume_ratio'), '.2f')}"
            )
            flags = row.get("flags", row.get("notes", []))
            if flags:
                typer.echo(f"      flags={','.join(flags)}")


@app.command("continuation-backtest")
def continuation_backtest_cmd(
    codes: str = typer.Argument(..., help="逗号分隔股票代码"),
    start: str = typer.Argument(..., help="回测开始日期 YYYY-MM-DD"),
    end: str = typer.Argument(..., help="回测结束日期 YYYY-MM-DD"),
    hold_days: int = typer.Option(2, help="持有天数"),
    top_n: int = typer.Option(3, help="每日保留 Top N"),
    db_path: Optional[Path] = typer.Option(None, help="数据库路径"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """运行短线续涨 Top N 回测。"""
    from hermes.backtest.continuation_backtest import run_continuation_backtest

    result = run_continuation_backtest(
        codes=[c.strip() for c in codes.split(",") if c.strip()],
        start=start,
        end=end,
        hold_days=hold_days,
        top_n=top_n,
        db_path=db_path,
    )

    if as_json:
        typer.echo(json.dumps(result, ensure_ascii=False, indent=2))
        return

    typer.echo(f"短线续涨回测 {start} ~ {end}")
    typer.echo(f"  Hold days: {result['hold_days']}  Top N: {result['top_n']}")
    typer.echo(
        f"  Total return: {result['total_return_pct']:.2f}%  Win rate: {result['win_rate_pct']:.2f}%"
    )
    typer.echo(f"  Trades: {len(result['trades'])}")


@app.command("continuation-study")
def continuation_study_cmd(
    codes: str = typer.Argument(..., help="逗号分隔股票代码"),
    start: str = typer.Option(..., help="研究开始日期 YYYY-MM-DD"),
    end: str = typer.Option(..., help="研究结束日期 YYYY-MM-DD"),
    top_ns: str = typer.Option("1,2,3", help="需要比较的 Top N 组合，如 1,2,3"),
    hold_days: str = typer.Option("1,2,3", help="需要比较的持有天数，如 1,2,3"),
    db_path: Optional[Path] = typer.Option(None, help="数据库路径"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """运行短线续涨收益研究，比较 Top N 与持有天数组合。"""
    from hermes.research.continuation_study import run_continuation_study

    result = run_continuation_study(
        codes=[c.strip() for c in codes.split(",") if c.strip()],
        start=start,
        end=end,
        top_ns=_parse_int_csv(top_ns),
        hold_days_list=_parse_int_csv(hold_days),
        db_path=db_path,
    )

    if as_json:
        typer.echo(json.dumps(result, ensure_ascii=False, indent=2))
        return

    typer.echo(f"短线续涨收益研究 {start} ~ {end}")
    typer.echo(f"  Top Ns: {','.join(str(v) for v in result['top_ns'])}")
    typer.echo(f"  Hold days: {','.join(str(v) for v in result['hold_days_list'])}")
    best = result.get("best_setup")
    if best:
        typer.echo(
            f"  Best: Top{best['top_n']} / 持有{best['hold_days']}天 "
            f"total={best['total_return_pct']:.2f}% "
            f"win={best['win_rate_pct']:.2f}% "
            f"avg={best['avg_trade_return_pct']:.2f}%"
        )
    typer.echo("  Comparison:")
    for row in result["comparison_report"]:
        typer.echo(
            f"    Top{row['top_n']} / 持有{row['hold_days']}天 "
            f"trades={row['trade_count']} days={row['trading_days']} "
            f"total={row['total_return_pct']:.2f}% "
            f"win={row['win_rate_pct']:.2f}% "
            f"avg={row['avg_trade_return_pct']:.2f}%"
        )


@app.command("score")
def score_stock(
    code: str = typer.Argument(..., help="股票代码"),
    db_path: Optional[Path] = typer.Option(None, help="数据库路径"),
):
    """单股四维评分（V2 纯函数引擎）"""
    import asyncio
    conn = connect(db_path)
    try:
        from hermes.market.service import MarketService
        from hermes.market.adapters import AkShareMarketAdapter, AkShareFinancialAdapter, AkShareFlowAdapter, MXMarketAdapter, MXSentimentAdapter
        from hermes.market.store import MarketStore
        from hermes.strategy.models import ScoringWeights
        from hermes.strategy.scorer import Scorer

        store = MarketStore(conn)
        market_svc = MarketService(
            market_providers=[MXMarketAdapter(), AkShareMarketAdapter()],
            financial_providers=[AkShareFinancialAdapter()],
            flow_providers=[AkShareFlowAdapter()],
            sentiment_providers=[MXSentimentAdapter()],
            store=store,
        )

        registry = ConfigRegistry()
        try:
            snapshot = registry.freeze(conn)
            cfg = snapshot.data.get("strategy", {})
        except Exception:
            cfg = {}

        weights_cfg = cfg.get("scoring", {}).get("weights", {})
        scorer = Scorer(
            weights=ScoringWeights(
                technical=weights_cfg.get("technical", 3),
                fundamental=weights_cfg.get("fundamental", 2),
                flow=weights_cfg.get("flow", 2),
                sentiment=weights_cfg.get("sentiment", 3),
            ),
            veto_rules=cfg.get("scoring", {}).get("veto", []),
            entry_cfg=cfg.get("entry_signal", {}),
        )

        snap = asyncio.run(market_svc.collect_snapshot(code))
        result = scorer.score(snap)

        typer.echo(f"{result.name}({result.code}): 总分 {result.total:.1f}")
        for d in result.dimensions:
            typer.echo(f"  {d.name}={d.score:.1f}/{d.max_score:.0f}  {d.detail}")
        typer.echo(f"  风格={result.style.value}  入场信号={'✅' if result.entry_signal else '❌'}")
        if result.veto_triggered:
            typer.echo(f"  ❌ 否决: {','.join(result.hard_veto)}")
    except Exception as e:
        typer.echo(f"评分失败: {e}", err=True)
        raise typer.Exit(1)
    finally:
        conn.close()


@app.command("status")
def portfolio_status(
    db_path: Optional[Path] = typer.Option(None, help="数据库路径"),
):
    """持仓概览"""
    conn = connect(db_path)
    try:
        store = EventStore(conn)
        from hermes.execution.service import ExecutionService
        svc = ExecutionService(store, conn)
        portfolio = svc.get_portfolio()
        positions = portfolio.get("positions", [])
        if not positions:
            typer.echo("当前无持仓")
            return
        typer.echo(f"持仓 {portfolio['holding_count']} 只:")
        for p in positions:
            cost = p["avg_cost_cents"] / 100
            typer.echo(f"  {p['code']} {p['name']}  {p['shares']}股  成本{cost:.2f}  风格={p['style']}")
    finally:
        conn.close()


@app.command("record-sell")
def record_sell(
    code: str = typer.Argument(..., help="股票代码，如 002261"),
    shares: int = typer.Argument(..., help="卖出股数（当前必须等于持仓数量）"),
    price: float = typer.Argument(..., help="成交价，如 34.52"),
    fee: float = typer.Option(0, "--fee", help="手续费（元），默认 0"),
    reason: str = typer.Option("manual", "--reason", help="卖出原因"),
    db_path: Optional[Path] = typer.Option(None, help="数据库路径"),
    yes: bool = typer.Option(False, "--yes", "-y", help="确认执行（必填）"),
):
    """录入已在券商 App 成交的卖出记录（手动补录，不调 broker）。"""
    from hermes.execution.service import ExecutionService

    conn = connect(db_path)
    try:
        store = EventStore(conn)
        svc = ExecutionService(store, conn)
        price_cents = int(price * 100)
        fee_cents = int(fee * 100)

        pos = svc.get_position(code)
        if not pos:
            raise ValueError(f"未找到持仓：{code}")

        # 预览
        proceeds = price_cents * shares - fee_cents
        pnl = (price_cents - pos.avg_cost_cents) * shares
        pnl_pct = (price - pos.avg_cost) / pos.avg_cost * 100

        typer.echo("═" * 50)
        typer.echo(f"  卖出录入预览")
        typer.echo("─" * 50)
        typer.echo(f"  股票       {code}  {pos.name}")
        typer.echo(f"  持仓       {pos.shares} 股")
        typer.echo(f"  卖出       {shares} 股")
        typer.echo(f"  成交价     ¥{price}")
        typer.echo(f"  成交额     ¥{proceeds / 100:,.2f}")
        typer.echo(f"  手续费     ¥{fee_cents / 100:.2f}")
        typer.echo(f"  成本       ¥{pos.avg_cost:.2f}")
        typer.echo(f"  盈亏       ¥{pnl / 100:+,.2f}  ({pnl_pct:+.1f}%)")
        typer.echo("─" * 50)

        if shares != pos.shares:
            raise ValueError(
                f"部分卖出暂不支持。当前持仓 {pos.shares} 股，传入了 {shares} 股。"
                f"\n如需卖出，请传入 --shares {pos.shares}"
            )

        if not yes:
            typer.echo("⚠️  添加 --yes 确认执行")
            raise typer.Abort()

        order = svc.record_sell(
            code=code,
            shares=shares,
            price_cents=price_cents,
            fee_cents=fee_cents,
            reason=reason,
        )

        conn.commit()
        typer.echo(f"✅ 已录入卖出：{code} {shares}股 @{price}")
        typer.echo(f"   订单ID：{order.order_id}")
    except Exception as e:
        typer.secho(f"❌ {e}", fg="red")
        raise typer.Abort()
    finally:
        conn.close()


@app.command("record-buy")
def record_buy(
    code: str = typer.Argument(..., help="股票代码，如 002261"),
    shares: int = typer.Argument(..., help="买入股数"),
    price: float = typer.Argument(..., help="成交价，如 39.91"),
    fee: float = typer.Option(0, "--fee", help="手续费（元），默认 0"),
    reason: str = typer.Option("manual", "--reason", help="买入原因"),
    name: str = typer.Option("", "--name", help="股票名称（可选）"),
    style: str = typer.Option("growth", "--style", help="风格：growth / momentum / slow_bull"),
    db_path: Optional[Path] = typer.Option(None, help="数据库路径"),
    yes: bool = typer.Option(False, "--yes", "-y", help="确认执行（必填）"),
):
    """录入已在券商 App 成交的买入记录（手动补录，不调 broker）。"""
    from hermes.execution.service import ExecutionService

    conn = connect(db_path)
    try:
        store = EventStore(conn)
        svc = ExecutionService(store, conn)
        price_cents = int(price * 100)
        fee_cents = int(fee * 100)
        total_cost = price_cents * shares + fee_cents

        typer.echo("═" * 50)
        typer.echo(f"  买入录入预览")
        typer.echo("─" * 50)
        typer.echo(f"  股票       {code}")
        typer.echo(f"  名称       {name or '(未填)'}")
        typer.echo(f"  风格       {style}")
        typer.echo(f"  买入       {shares} 股")
        typer.echo(f"  成交价     ¥{price}")
        typer.echo(f"  成交额     ¥{price_cents * shares / 100:,.2f}")
        typer.echo(f"  手续费     ¥{fee_cents / 100:.2f}")
        typer.echo(f"  总成本     ¥{total_cost / 100:,.2f}")
        typer.echo("─" * 50)

        if not yes:
            typer.echo("⚠️  添加 --yes 确认执行")
            raise typer.Abort()

        order = svc.record_buy(
            code=code,
            shares=shares,
            price_cents=price_cents,
            fee_cents=fee_cents,
            reason=reason,
        )

        conn.commit()
        typer.echo(f"✅ 已录入买入：{code} {shares}股 @{price}")
        typer.echo(f"   订单ID：{order.order_id}")
    except Exception as e:
        typer.secho(f"❌ {e}", fg="red")
        raise typer.Abort()
    finally:
        conn.close()


@app.command("mcp")
def run_mcp():
    """启动 MCP Server（stdio transport）"""
    from hermes.platform.mcp_server import main as mcp_main
    mcp_main()


@app.command("run-pipeline")
def run_pipeline(
    pipeline_type: str = typer.Argument(..., help="morning | noon | intraday_monitor | evening | scoring | weekly"),
    db_path: Optional[Path] = typer.Option(None, help="数据库路径"),
):
    """运行指定 pipeline（完整流程，带幂等检查）"""
    from hermes.pipeline.context import build_context

    ctx = build_context(db_path)
    try:
        multi_run_pipelines = {"sentiment", "intraday_monitor"}
        if pipeline_type not in multi_run_pipelines and ctx.run_journal.is_completed_today(pipeline_type):
            typer.echo(f"⏭️  {pipeline_type} 今日已完成，跳过")
            return

        run_id = ctx.run_journal.start_run(pipeline_type, ctx.config_version)
        typer.echo(f"▶️  {pipeline_type} 开始 (run_id={run_id})")

        try:
            if pipeline_type == "morning":
                from hermes.pipeline.morning import run
                result = run(ctx, run_id)
                typer.echo(f"  大盘={result['signal']} 持仓={result['positions']} 风控={len(result['risk_alerts'])}条")

            elif pipeline_type == "noon":
                from hermes.pipeline.noon import run
                result = run(ctx, run_id)
                typer.echo(f"  大盘={result['signal']} 持仓={result['positions']} 风控={len(result['alerts'])}条")

            elif pipeline_type == "intraday_monitor":
                from hermes.pipeline.intraday_monitor import run
                result = run(ctx, run_id)
                typer.echo(
                    f"  持仓={result['positions']} "
                    f"新告警={len(result['alerts'])}条 去重={result['deduped']}条"
                )

            elif pipeline_type == "scoring":
                from hermes.pipeline.scoring import run
                result = run(ctx, run_id)
                typer.echo(f"  评分 {result['scored']} 只股票")

            elif pipeline_type == "evening":
                from hermes.pipeline.evening import run
                result = run(ctx, run_id)
                typer.echo(f"  大盘={result['signal']} 持仓={result['positions']} 风控={len(result['risk_alerts'])}条")

            elif pipeline_type == "weekly":
                from hermes.pipeline.weekly import run
                result = run(ctx, run_id)
                typer.echo(f"  {result['buy_count']}买 {result['sell_count']}卖 胜率{result['win_rate']:.0%}")

            elif pipeline_type == "sentiment":
                from hermes.pipeline.sentiment import run as sentiment_run
                result = sentiment_run(ctx, run_id)
                typer.echo(f"  监控{result['monitored']}只 告警{len(result['alerts'])}条")

            elif pipeline_type == "auto_trade":
                from hermes.pipeline.auto_trade import run as auto_trade_run
                result = auto_trade_run(ctx, run_id)
                if not result.get("enabled"):
                    typer.echo("  ⏭️ auto_trade 未启用")
                else:
                    mode = "[DRY]" if result.get("dry_run") else ""
                    typer.echo(f"  {mode} 买入{len(result['buys'])}笔 卖出{len(result['sells'])}笔")

            else:
                ctx.run_journal.fail_run(run_id, f"Unknown pipeline: {pipeline_type}")
                typer.echo(f"❌ Unknown pipeline: {pipeline_type}", err=True)
                raise typer.Exit(1)

            ctx.run_journal.complete_run(run_id, artifacts={"result": "ok"})
            typer.echo(f"✅ {pipeline_type} 完成")

        except Exception as e:
            ctx.run_journal.fail_run(run_id, str(e))
            typer.echo(f"❌ {pipeline_type} 失败: {e}", err=True)
            raise typer.Exit(1)
    finally:
        ctx.conn.close()


@app.command("refresh-positions")
def refresh_positions_cmd(
    db_path: Optional[Path] = typer.Option(None, help="数据库路径"),
):
    """刷新持仓实时价格并写 DB（自动跳过缓存未过期的）。"""
    from hermes.pipeline.context import build_context

    ctx = build_context(db_path)
    try:
        from hermes.pipeline.helpers import refresh_position_prices

        prices = refresh_position_prices(ctx)
        if not prices:
            typer.echo("无持仓")
        else:
            typer.echo(f"已刷新 {len(prices)} 只持仓:")
            for code, price in prices.items():
                typer.echo(f"  {code}  ¥{price:.2f}")
    finally:
        ctx.conn.close()


def main():
    app()


if __name__ == "__main__":
    main()
