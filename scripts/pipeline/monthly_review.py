#!/usr/bin/env python3
"""
pipeline/monthly_review.py — 月度复盘自动生成

从结构化账本统计月度数据，生成 03-分析/月复盘/YYYY-MM.md。

功能：
  - 月度 P&L、胜率、盈亏比、最大回撤
  - 周度汇总表（4-5 周）
  - 亏损最大 3 笔交易 + 原因
  - 核心池月度变化
  - 模拟盘 vs 实盘对比
  - 系统参数检查建议

用法：
  python -m scripts.pipeline.monthly_review                # 当月
  python -m scripts.pipeline.monthly_review --month 2026-04  # 指定月份
  bin/trade run monthly --json

CLI 集成：
  bin/trade run monthly --json
  bin/trade run monthly --month 2026-04 --json
"""

import json
import os
import sys
from datetime import date, datetime

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from scripts.state import (
    load_pool_action_history,
)
from scripts.utils.common import _safe_float
from scripts.utils.config_loader import get_strategy
from scripts.utils.obsidian import ObsidianVault
from scripts.utils.logger import get_logger
from scripts.utils.runtime_state import update_pipeline_state
from scripts.utils.trading_calendar import trading_days_in_month

_logger = get_logger("pipeline.monthly_review")


def _iso_week(d: date) -> str:
    return f"{d.isocalendar()[0]}-W{d.isocalendar()[1]:02d}"


def _load_monthly_trades(month_str: str, scope: str = "cn_a_system") -> list[dict]:
    """从结构化账本读取指定月份的交易事件"""
    try:
        from scripts.state.service import _connect, _ensure_bootstrapped
        year, month = int(month_str[:4]), int(month_str[5:7])
        start = f"{year}-{month:02d}-01"
        if month == 12:
            end = f"{year + 1}-01-01"
        else:
            end = f"{year}-{month + 1:02d}-01"

        with _connect() as conn:
            _ensure_bootstrapped(conn)
            rows = conn.execute(
                "SELECT * FROM trade_events WHERE event_date >= ? AND event_date < ? AND scope = ? ORDER BY event_date, created_at",
                (start, end, scope),
            ).fetchall()
            events = []
            for row in rows:
                event = dict(row)
                event["metadata"] = json.loads(event.pop("metadata_json", "{}") or "{}")
                events.append(event)
            return events
    except Exception as exc:
        _logger.warning(f"[monthly] 读取交易事件失败: {exc}")
        return []


def _load_monthly_paper_trades(month_str: str) -> list[dict]:
    """读取模拟盘月度交易事件"""
    return _load_monthly_trades(month_str, scope="paper_mx")


def _compute_stats(events: list[dict]) -> dict:
    """从交易事件计算月度统计"""
    buy_count = 0
    sell_count = 0
    total_pnl = 0.0
    wins = []
    losses = []
    daily_pnl = {}

    for e in events:
        side = str(e.get("side", "")).lower()
        if side == "buy":
            buy_count += 1
        elif side == "sell":
            sell_count += 1
            pnl = _safe_float(e.get("realized_pnl", 0))
            total_pnl += pnl
            if pnl > 0:
                wins.append(e)
            elif pnl < 0:
                losses.append(e)
            day = str(e.get("event_date", ""))[:10]
            if day:
                daily_pnl[day] = daily_pnl.get(day, 0.0) + pnl

    trade_count = buy_count + sell_count
    closed_count = sell_count
    win_rate = len(wins) / closed_count if closed_count > 0 else 0
    avg_win = sum(_safe_float(w.get("realized_pnl", 0)) for w in wins) / len(wins) if wins else 0
    avg_loss = sum(_safe_float(l.get("realized_pnl", 0)) for l in losses) / len(losses) if losses else 0
    profit_loss_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else 0

    # 最大回撤（基于日度累计 P&L）
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    sorted_days = sorted(daily_pnl.keys())
    for day in sorted_days:
        cumulative += daily_pnl[day]
        if cumulative > peak:
            peak = cumulative
        dd = peak - cumulative
        if dd > max_drawdown:
            max_drawdown = dd

    # 亏损最大 3 笔
    worst_trades = sorted(losses, key=lambda x: _safe_float(x.get("realized_pnl", 0)))[:3]

    return {
        "buy_count": buy_count,
        "sell_count": sell_count,
        "trade_count": trade_count,
        "closed_count": closed_count,
        "total_pnl": round(total_pnl, 2),
        "win_count": len(wins),
        "loss_count": len(losses),
        "win_rate": round(win_rate, 4),
        "avg_win": round(avg_win, 2),
        "avg_loss": round(avg_loss, 2),
        "profit_loss_ratio": round(profit_loss_ratio, 2),
        "max_drawdown": round(max_drawdown, 2),
        "daily_pnl": daily_pnl,
        "worst_trades": worst_trades,
    }


def _build_report(month_str: str, real_stats: dict, paper_stats: dict,
                  real_events: list, paper_events: list,
                  pool_actions: list, strategy: dict,
                  trading_day_count: int) -> str:
    """生成月度复盘 Markdown"""
    year, month = int(month_str[:4]), int(month_str[5:7])

    lines = [
        "---",
        f"date: {month_str}-28",
        "type: monthly_review",
        f"tags: [月复盘]",
        f"month: {month_str}",
        f"updated_at: {datetime.now().strftime('%Y-%m-%d')}",
        "---",
        "",
        f"# {year}年{month}月 月度复盘",
        "",
    ]

    # 月度概览
    lines.extend([
        "## 月度概览",
        "",
        "| 项目 | 实盘 | 模拟盘 |",
        "|------|------|--------|",
        f"| 交易日数 | {trading_day_count} | {trading_day_count} |",
        f"| 买入次数 | {real_stats['buy_count']} | {paper_stats['buy_count']} |",
        f"| 卖出次数 | {real_stats['sell_count']} | {paper_stats['sell_count']} |",
        f"| 已实现盈亏 | ¥{real_stats['total_pnl']:+,.2f} | ¥{paper_stats['total_pnl']:+,.2f} |",
        f"| 胜率 | {real_stats['win_rate']:.0%} | {paper_stats['win_rate']:.0%} |",
        f"| 盈亏比 | {real_stats['profit_loss_ratio']:.2f} | {paper_stats['profit_loss_ratio']:.2f} |",
        f"| 最大回撤 | ¥{real_stats['max_drawdown']:,.2f} | ¥{paper_stats['max_drawdown']:,.2f} |",
        f"| 平均盈利 | ¥{real_stats['avg_win']:+,.2f} | ¥{paper_stats['avg_win']:+,.2f} |",
        f"| 平均亏损 | ¥{real_stats['avg_loss']:+,.2f} | ¥{paper_stats['avg_loss']:+,.2f} |",
        "",
    ])

    # 周度汇总
    lines.extend(["## 周度汇总", ""])
    week_data = {}
    for e in real_events:
        day = str(e.get("event_date", ""))[:10]
        if not day:
            continue
        try:
            d = datetime.strptime(day, "%Y-%m-%d").date()
            wk = _iso_week(d)
        except ValueError:
            continue
        if wk not in week_data:
            week_data[wk] = {"pnl": 0.0, "buy": 0, "sell": 0, "wins": 0, "losses": 0}
        side = str(e.get("side", "")).lower()
        if side == "buy":
            week_data[wk]["buy"] += 1
        elif side == "sell":
            week_data[wk]["sell"] += 1
            pnl = _safe_float(e.get("realized_pnl", 0))
            week_data[wk]["pnl"] += pnl
            if pnl > 0:
                week_data[wk]["wins"] += 1
            elif pnl < 0:
                week_data[wk]["losses"] += 1

    lines.append("| 周次 | 盈亏 | 买入 | 卖出 | 胜/负 |")
    lines.append("|------|------|------|------|-------|")
    for wk in sorted(week_data.keys()):
        wd = week_data[wk]
        total = wd["wins"] + wd["losses"]
        wr = f"{wd['wins']}/{wd['losses']}" if total > 0 else "—"
        lines.append(f"| {wk} | ¥{wd['pnl']:+,.2f} | {wd['buy']} | {wd['sell']} | {wr} |")
    if not week_data:
        lines.append("| — | — | — | — | 本月无交易 |")
    lines.append("")

    # 亏损最大 3 笔
    lines.extend(["## 亏损最大的 3 笔交易", ""])
    worst = real_stats["worst_trades"]
    if worst:
        lines.append("| 日期 | 股票 | 代码 | 卖出价 | 数量 | 亏损 | 原因 |")
        lines.append("|------|------|------|--------|------|------|------|")
        for t in worst:
            reason = str(t.get("reason_text", t.get("reason_code", ""))).strip() or "—"
            lines.append(
                f"| {str(t.get('event_date', ''))[:10]} | {t.get('name', '—')} | {t.get('code', '—')} | "
                f"¥{_safe_float(t.get('price', 0)):.2f} | {int(t.get('shares', 0) or 0)} | "
                f"¥{_safe_float(t.get('realized_pnl', 0)):+,.2f} | {reason} |"
            )
    else:
        lines.append("本月无亏损交易。")
    lines.append("")

    # 模拟盘 vs 实盘对比
    lines.extend(["## 模拟盘 vs 实盘对比", ""])
    if paper_stats["trade_count"] > 0 or real_stats["trade_count"] > 0:
        signal_count = paper_stats["buy_count"]
        executed_count = real_stats["buy_count"]
        divergence = signal_count - executed_count
        lines.append(f"- 模拟盘发出 **{signal_count}** 个买入信号")
        lines.append(f"- 实盘执行了 **{executed_count}** 个买入")
        if divergence > 0:
            lines.append(f"- 偏离：**{divergence}** 个信号未执行（实盘更保守）")
        elif divergence < 0:
            lines.append(f"- 偏离：实盘多执行了 **{abs(divergence)}** 个买入（实盘更激进）")
        else:
            lines.append("- 偏离：无，完全一致 ✅")
        pnl_diff = real_stats["total_pnl"] - paper_stats["total_pnl"]
        lines.append(f"- 盈亏差异：实盘 vs 模拟盘 = ¥{pnl_diff:+,.2f}")
    else:
        lines.append("本月无交易数据可对比。")
    lines.append("")

    # 核心池月度变化
    lines.extend(["## 核心池月度变化", ""])
    if pool_actions:
        lines.append("| 日期 | 股票 | 代码 | 操作 | 原因 |")
        lines.append("|------|------|------|------|------|")
        for a in pool_actions[:20]:
            lines.append(
                f"| {str(a.get('snapshot_date', a.get('updated_at', '')))[:10]} | "
                f"{a.get('name', '—')} | {a.get('code', '—')} | "
                f"{a.get('action', '—')} | {a.get('reason_text', '—')} |"
            )
    else:
        lines.append("本月无核心池变动。")
    lines.append("")

    # 系统参数检查
    risk = strategy.get("risk", {})
    position = risk.get("position", {})
    tp = risk.get("take_profit", {})
    lines.extend([
        "## 系统参数检查",
        "",
        "| 参数 | 当前值 | 是否需要调整 | 备注 |",
        "|------|--------|-------------|------|",
        f"| 止损线 | {risk.get('stop_loss', 0.04):.0%} / {risk.get('absolute_stop', 0.07):.0%} | — | 动态/绝对 |",
        f"| 止盈第一批 | {tp.get('t1_pct', 0.15):.0%} | — | +15%卖1/3 |",
        f"| 时间止损 | {risk.get('time_stop_days', 15)}天 | — | 振幅<2%触发 |",
        f"| 每周买入上限 | {position.get('weekly_max', 2)}次 | — | — |",
        f"| 总仓位上限 | {position.get('total_max', 0.6):.0%} | — | — |",
        f"| 单票上限 | {position.get('single_max', 0.2):.0%} | — | — |",
        "",
        "> 参数调整需基于至少 20 笔闭合交易的 MFE/MAE 分布，当前样本量不足时保持默认值。",
        "",
    ])

    # 下月计划
    lines.extend([
        "## 下月计划",
        "",
        "> ",
        "",
    ])

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------

def run(month: str | None = None) -> dict:
    """
    生成月度复盘报告。

    Args:
        month: "YYYY-MM" 格式，默认当月

    Returns:
        {"status": "ok", "month": "2026-04", "report_path": "...", "stats": {...}}
    """
    if not month:
        month = datetime.now().strftime("%Y-%m")

    today_str = datetime.now().strftime("%Y-%m-%d")
    _logger.info(f"[MONTHLY] 月度复盘 {month}")

    try:
        vault = ObsidianVault()
        strategy = get_strategy()

        # 交易日数
        td = trading_days_in_month(month)
        trading_day_count = len(td)
        _logger.info(f"  交易日数: {trading_day_count}")

        # 实盘交易事件
        _logger.info(">> 读取实盘交易事件...")
        real_events = _load_monthly_trades(month, scope="cn_a_system")
        real_stats = _compute_stats(real_events)
        _logger.info(f"  实盘: {real_stats['trade_count']} 笔, P&L ¥{real_stats['total_pnl']:+,.2f}")

        # 模拟盘交易事件
        _logger.info(">> 读取模拟盘交易事件...")
        paper_events = _load_monthly_paper_trades(month)
        paper_stats = _compute_stats(paper_events)
        _logger.info(f"  模拟盘: {paper_stats['trade_count']} 笔, P&L ¥{paper_stats['total_pnl']:+,.2f}")

        # 核心池变动
        _logger.info(">> 读取核心池变动...")
        pool_actions_raw = load_pool_action_history(limit=100)
        pool_actions = [
            a for a in pool_actions_raw.get("actions", [])
            if str(a.get("snapshot_date", "")).startswith(month)
        ]
        _logger.info(f"  核心池变动: {len(pool_actions)} 条")

        # 生成报告
        _logger.info(">> 生成月度报告...")
        report_content = _build_report(
            month, real_stats, paper_stats,
            real_events, paper_events,
            pool_actions, strategy,
            trading_day_count,
        )

        report_relative = f"{vault.monthly_review_dir}/{month}.md"
        vault.write(report_relative, report_content)
        report_path = f"{vault.vault_path}/{report_relative}"
        _logger.info(f"  已写入: {report_path}")

        update_pipeline_state(
            "monthly_review",
            "success",
            {
                "month": month,
                "report_path": str(report_path),
                "trading_day_count": trading_day_count,
                "real_trade_count": real_stats["trade_count"],
                "paper_trade_count": paper_stats["trade_count"],
                "real_pnl": real_stats["total_pnl"],
                "paper_pnl": paper_stats["total_pnl"],
            },
            today_str,
        )

        _logger.info(f"[MONTHLY] 月度复盘完成 → {report_path}")

        return {
            "status": "ok",
            "month": month,
            "report_path": str(report_path),
            "trading_day_count": trading_day_count,
            "real_stats": real_stats,
            "paper_stats": paper_stats,
            "pool_action_count": len(pool_actions),
        }
    except Exception as e:
        update_pipeline_state(
            "monthly_review",
            "error",
            {"month": month, "error": str(e)},
            today_str,
        )
        raise


if __name__ == "__main__":
    m = None
    for arg in sys.argv[1:]:
        if arg.startswith("--month"):
            continue
        if arg.startswith("2"):
            m = arg
    if "--month" in sys.argv:
        idx = sys.argv.index("--month")
        if idx + 1 < len(sys.argv):
            m = sys.argv[idx + 1]
    result = run(month=m)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
