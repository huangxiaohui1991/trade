#!/usr/bin/env python3
from typing import Optional, Union
"""
pipeline/weekly_review.py — 周报生成（周日 20:00 执行）

职责：
  1. 读取结构化活动摘要（load_activity_summary）
  2. 统计：本周买入次数 + 卖出次数 + P&L + 胜率 + 盈亏比
  3. 计算：核心池变化
  4. 输出到 vault/03-分析/周复盘/YYYY-W##.md
  5. Discord 推送周报

用法（CLI）：
  python -m scripts.pipeline.weekly_review

用法（导入）：
  from scripts.pipeline.weekly_review import run
  result = run()
"""

import os
import sys
import warnings
from datetime import datetime, timedelta, date as date_cls
from pathlib import Path

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

os.environ["TQDM_DISABLE"] = "1"
warnings.filterwarnings("ignore")

from scripts.utils.common import _safe_float
from scripts.utils.config_loader import get_stocks
from scripts.utils.parser import parse_md_table
from scripts.utils.obsidian import ObsidianVault
from scripts.utils.discord_push import send_weekly_report
from scripts.utils.logger import get_logger
from scripts.utils.runtime_state import update_pipeline_state
from scripts.engine.trading_record import load_activity_summary
from scripts.state import load_trade_review, get_capital_for_date

_logger = get_logger("pipeline.weekly_review")


def _extract_report_scores(report_path: Path) -> dict:
    """从评分报告中提取 代码 -> 总分 映射。"""
    with open(report_path, encoding="utf-8") as f:
        tables = parse_md_table(f.read())

    scores = {}
    for table in tables:
        for row in table.get("rows", []):
            code = str(row.get("代码", "")).strip()
            if not code:
                continue

            raw_score = (
                row.get("**总分(10)**")
                or row.get("总分")
                or row.get("四维总分")
                or ""
            )
            if isinstance(raw_score, str):
                raw_score = raw_score.replace("**", "").strip()
            try:
                scores[code] = float(raw_score)
            except (ValueError, TypeError):
                continue

    return scores


def _safe_date_key(event: dict) -> str:
    """从事件里提取 YYYY-MM-DD 作为分组键。"""
    trade_date = str(
        event.get("trade_date")
        or event.get("date")
        or event.get("timestamp")
        or ""
    ).strip()
    if len(trade_date) >= 10:
        return trade_date[:10]
    return trade_date


def _build_weekly_report(vault: ObsidianVault, stats: dict,
                         core_pool_changes: list, trade_events: list,
                         year: int, week_num: int,
                         shadow_advisories: Optional[list] = None,
                         trade_review: Optional[dict] = None) -> str:
    """生成周报 markdown 内容"""
    week_str = f"{year}-W{week_num:02d}"

    # 计算本周周一到周五（基于 ISO 周）
    jan4 = date_cls(year, 1, 4)
    monday = jan4 + timedelta(weeks=week_num - 1, days=-jan4.weekday())
    friday = monday + timedelta(days=4)

    # 查询周初/周末资产（实盘 = A股 + 港股合并）
    week_start = get_capital_for_date(monday.isoformat(), "merged")
    week_end = get_capital_for_date(friday.isoformat(), "merged")
    week_start_str = f"¥{week_start['total_capital']:,.2f}" if week_start["found"] else "¥（无快照）"
    week_end_str = f"¥{week_end['total_capital']:,.2f}" if week_end["found"] else "¥（无快照）"

    # 计算收益率
    weekly_return_pct = 0.0
    if week_start["found"] and week_end["found"] and week_start["total_capital"] > 0:
        weekly_return_pct = round((week_end["total_capital"] - week_start["total_capital"]) / week_start["total_capital"] * 100, 2)

    trade_events = trade_events or stats.get("trade_events", [])
    pnl_abs = _safe_float(stats.get("realized_pnl", stats.get("total_pnl", 0)))
    buy_count = int(stats.get("weekly_buy_count", stats.get("buy_count", 0)) or 0)
    sell_count = int(stats.get("weekly_sell_count", stats.get("sell_count", 0)) or 0)
    trade_count = int(stats.get("trade_count", len(trade_events)) or 0)

    day_pnl = {}
    for event in trade_events:
        day_key = _safe_date_key(event)
        if not day_key:
            continue
        pnl = _safe_float(event.get("realized_pnl", event.get("pnl", 0)))
        if event.get("action") == "SELL":
            day_pnl[day_key] = day_pnl.get(day_key, 0.0) + pnl

    win_days = sum(1 for pnl in day_pnl.values() if pnl > 0)
    loss_days = sum(1 for pnl in day_pnl.values() if pnl < 0)
    total_days = win_days + loss_days
    win_rate = (win_days / total_days * 100) if total_days > 0 else 0

    # 盈亏比（简化：平均盈利日 vs 平均亏损日）
    avg_win = 0.0
    avg_loss = 0.0
    win_count = 0
    loss_count = 0
    for pnl in day_pnl.values():
        if pnl > 0:
            avg_win += pnl
            win_count += 1
        elif pnl < 0:
            avg_loss += pnl
            loss_count += 1
    if win_count > 0:
        avg_win /= win_count
    if loss_count > 0:
        avg_loss /= loss_count
    profit_loss_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else 0

    active_days = len(day_pnl)

    lines = [
        f"# {week_str} 周复盘（{monday.strftime('%m/%d')} - {friday.strftime('%m/%d')}）",
        "",
        "---",
        "",
        "## 本周概览（自动统计）",
        "",
        "| 项目 | 数据 |",
        "|------|------|",
        f"| 周初资产 | {week_start_str} |",
        f"| 周末资产 | {week_end_str} |",
        f"| 本周盈亏 | ¥{pnl_abs:+,.2f}（自动统计） |",
        f"| 收益率 | {weekly_return_pct:+.2f}% |",
        f"| 主动买入次数 | {buy_count} 次（结构化事件） |",
        f"| 主动卖出次数 | {sell_count} 次（结构化事件） |",
        f"| 交易事件数 | {trade_count} 条（结构化事件） |",
        f"| 盈利交易日 / 亏损交易日 | {win_days} / {loss_days}（结构化事件） |",
        f"| 胜率 | {win_rate:.0f}%（自动统计） |",
        f"| 盈亏比 | {profit_loss_ratio:.2f} |",
        "",
        "> 周初/周末资产由每日收盘快照自动提取，其余由系统从结构化交易事件自动统计。",
        "",
        "---",
        "",
        "## 本周交易明细（自动从结构化事件提取）",
        "",
        "| 日期 | 股票 | 代码 | 操作 | 价格 | 数量 | 盈亏 | 备注 |",
        "|------|------|------|------|------|------|------|------|",
    ]

    if trade_events:
        for event in trade_events:
            reason = str(event.get("reason", "")).strip()
            reason_code = str(event.get("reason_code", "")).strip()
            remark = reason
            if reason_code:
                remark = f"{reason_code} {remark}".strip()
            lines.append(
                f"| {_safe_date_key(event)} | {event.get('name', '—')} | "
                f"{event.get('code', '—')} | {event.get('action', '—')} | "
                f"¥{_safe_float(event.get('price', 0)):.2f} | {int(event.get('shares', 0) or 0)} | "
                f"¥{_safe_float(event.get('realized_pnl', 0)):+,.2f} | {remark or '—'} |"
            )
    else:
        lines.append("| — | — | — | — | — | — | — | 本周无交易事件 |")

    lines.extend(["", "---", "", "## Advisory 风控提示（影子盘）", ""])
    lines.append("")
    lines.append("> 时间止损 / 回撤止盈目前仅做提示，不自动执行。")
    lines.append("")
    if shadow_advisories:
        lines.append("| 股票 | 代码 | 持有天数 | 回撤 | 提示 |")
        lines.append("|------|------|----------|------|------|")
        for item in shadow_advisories:
            drawdown_pct = _safe_float(item.get("drawdown_pct", 0))
            lines.append(
                f"| {item.get('name', '—')} | {item.get('code', '—')} | "
                f"{item.get('hold_days', '—')} | {drawdown_pct*100:.1f}% | "
                f"{item.get('summary', '—')} |"
            )
    else:
        lines.append("（当前无时间止损 / 回撤止盈提示）")

    lines.extend(["", "---", "", "## 规则执行检查", ""])
    lines.extend([
        "- [ ] 是否遵守了所有买入规则？",
        "- [ ] 是否遵守了止损纪律？",
        "- [ ] 加仓操作是否合理？",
        "- [ ] 是否触发过冷却机制？",
        "- [ ] 是否有情绪化交易？",
        "",
        "**违反规则记录：**",
        "",
        "",
    ])

    lines.extend(["", "---", "", "## 复盘归因（结构化闭合交易）", ""])
    closed_trades = list((trade_review or {}).get("closed_trades", []))
    review_summary = dict((trade_review or {}).get("summary_stats", {}))
    lines.append("| 指标 | 数值 |")
    lines.append("|------|------|")
    lines.append(f"| 平均持有天数 | {float(review_summary.get('avg_holding_days', 0.0)):.1f} 天 |")
    lines.append(f"| 平均盈利单笔 | ¥{_safe_float(review_summary.get('avg_win', 0.0)):+,.2f} |")
    lines.append(f"| 平均亏损单笔 | ¥{_safe_float(review_summary.get('avg_loss', 0.0)):+,.2f} |")
    lines.append(f"| 规则违例数 | {int(review_summary.get('rule_break_count', 0) or 0)} |")
    attribution_summary = dict((trade_review or {}).get("portfolio_attribution_summary", {}))
    if attribution_summary:
        lines.append(f"| 出场风格盈亏 | {attribution_summary.get('pnl_by_exit_style', {})} |")
        lines.append(f"| 规则偏离分布 | {attribution_summary.get('rule_deviation_counts', {})} |")
    lines.append("")
    if closed_trades:
        lines.append("| 股票 | 开始 | 结束 | 持有天数 | 入场原因 | 出场原因 | 已实现盈亏 | 标签 |")
        lines.append("|------|------|------|----------|----------|----------|------------|------|")
        for item in closed_trades:
            entry_reason = (
                ",".join(item.get("entry_reason_codes", [])[:2])
                or item.get("entry_reason_code", "")
                or "—"
            )
            exit_reason = ",".join(item.get("exit_reason_codes", [])[:2]) or "—"
            tags = ",".join(item.get("rule_tags", [])) or "—"
            lines.append(
                f"| {item.get('name', '—')} | {item.get('entry_date', '—')} | {item.get('exit_date', '—')} | "
                f"{item.get('holding_days', '—')} | {entry_reason} | {exit_reason} | "
                f"¥{_safe_float(item.get('realized_pnl', 0)):+,.2f} | {tags} |"
            )
        lines.append("")
        lines.append(f"> MFE/MAE 暂未接入历史行情，当前状态：{(trade_review or {}).get('mfe_mae_status', 'pending')}")
    else:
        lines.append("（本周无闭合交易可归因）")

    # 核心池变化
    lines.extend(["", "---", "", "## 核心池变化", ""])
    if core_pool_changes:
        lines.append("| 股票 | 上周评分 | 本周评分 | 变化 | 原因 |")
        lines.append("|------|---------|---------|------|------|")
        for change in core_pool_changes:
            old = change.get("old_score", 0)
            new = change.get("new_score", 0)
            chg = new - old
            chg_mark = f"{'+' if chg >= 0 else ''}{chg:.1f}"
            lines.append(
                f"| {change.get('name', '')} | {old:.1f} | {new:.1f} | {chg_mark} | "
                f"{change.get('reason', '')} |"
            )
    else:
        lines.append("（无核心池变化）")

    lines.extend(["", "---", "", "## 下周计划（只写1条）", ""])
    lines.extend([
        "### 核心池",
        "",
        "| 股票 | 代码 | 计划操作 | 技术面状态 |",
        "|------|------|----------|-----------|",
    ])
    appended = 0
    try:
        vault_core = vault.read_core_pool()
        for row in vault_core:
            name = str(row.get("股票", "")).strip()
            code = str(row.get("代码", "")).strip()
            if name and name not in ["", "—"]:
                score = _safe_float(row.get("四维总分", row.get("总分", 0)))
                passed = str(row.get("通过", "")).strip()
                note = str(row.get("备注", "")).strip()

                if passed == "✅" or score >= 7:
                    action = "关注买入"
                    state = "✅ 可操作"
                elif passed == "🟡" or score >= 5:
                    action = "观察"
                    state = "🟡 观察"
                elif passed == "❌" or note.startswith("veto:") or score < 5:
                    action = "观察"
                    state = "❌ 规避"
                else:
                    action = "观察"
                    state = "🟡 观察"

                lines.append(f"| {name} | {code} | {action} | {state} |")
                appended += 1
    except Exception:
        pass
    if appended == 0:
        lines.append("| — | — | 观察 | 待更新核心池评分 |")
    lines.extend(["", "### 下周改进点", "", "> "])

    return "\n".join(lines), {
        "pnl_abs": pnl_abs,
        "win_rate": win_rate,
        "profit_loss_ratio": profit_loss_ratio,
        "weekly_buy_count": buy_count,
        "weekly_sell_count": sell_count,
        "trade_count": trade_count,
        "realized_pnl": pnl_abs,
        "active_days": active_days,
    }


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------

def run() -> dict:
    """执行周报生成"""
    now = datetime.now()
    year, week_num = now.isocalendar()[0], now.isocalendar()[1]
    week_str = f"{year}-W{week_num:02d}"

    _logger.info(f"[WEEKLY] 周报生成 {week_str}")

    try:
        vault = ObsidianVault()

        # 1. 统计本周数据
        _logger.info(">> 读取结构化活动摘要...")
        stats = load_activity_summary(7, scope="cn_a_system")
        trade_events = stats.get("trade_events", [])

        win_days = 0
        loss_days = 0
        day_pnl = {}
        for event in trade_events:
            day_key = _safe_date_key(event)
            if not day_key or event.get("action") != "SELL":
                continue
            day_pnl[day_key] = day_pnl.get(day_key, 0.0) + _safe_float(
                event.get("realized_pnl", event.get("pnl", 0))
            )
        win_days = sum(1 for pnl in day_pnl.values() if pnl > 0)
        loss_days = sum(1 for pnl in day_pnl.values() if pnl < 0)
        total_days = win_days + loss_days
        win_rate = (win_days / total_days * 100) if total_days > 0 else 0
        buy_count = int(stats.get("weekly_buy_count", stats.get("buy_count", 0)) or 0)
        sell_count = int(stats.get("weekly_sell_count", stats.get("sell_count", 0)) or 0)
        realized_pnl = _safe_float(stats.get("realized_pnl", 0))

        _logger.info(
            f"  结构化交易事件: {len(trade_events)} | "
            f"盈利{win_days}/亏损{loss_days} | "
            f"胜率{win_rate:.0f}% | "
            f"总盈亏: ¥{realized_pnl:+.2f} | "
            f"买入{buy_count}/卖出{sell_count}"
        )

        # 2. 核心池变化（从评分报告目录对比）
        _logger.info(">> 检查核心池变化...")
        core_pool_changes = []
        try:
            stock_name_map = {
                str(item.get("code", "")).strip(): str(item.get("name", "")).strip()
                for item in get_stocks().get("core_pool", [])
                if str(item.get("code", "")).strip()
            }
            for row in vault.read_core_pool():
                code = str(row.get("代码", "")).strip()
                name = str(row.get("股票", "")).strip()
                if code and name:
                    stock_name_map.setdefault(code, name)

            reports_dir = Path(vault.vault_path) / vault.screening_results_dir
            if reports_dir.exists():
                reports = sorted(reports_dir.glob("核心池_评分报告_*.md"), key=lambda p: p.stat().st_mtime)
                if len(reports) >= 2:
                    prev_report = reports[-2]
                    curr_report = reports[-1]

                    prev_scores = _extract_report_scores(prev_report)
                    curr_scores = _extract_report_scores(curr_report)

                    all_codes = set(list(prev_scores.keys()) + list(curr_scores.keys()))
                    for code in all_codes:
                        old = prev_scores.get(code, 0)
                        new = curr_scores.get(code, 0)
                        if old != new:
                            core_pool_changes.append({
                                "name": stock_name_map.get(code, code),
                                "old_score": old,
                                "new_score": new,
                                "reason": "评分上升" if new > old else "评分下降",
                            })
        except Exception as e:
            _logger.warning(f"  核心池变化检测失败: {e}")

        position_changes = []
        for event in trade_events:
            position_changes.append({
                "action": event.get("action", ""),
                "name": event.get("name", ""),
                "shares": event.get("shares", 0),
                "price": event.get("price", 0),
                "currency": "¥",
                "reason_code": event.get("reason_code", ""),
            })

        shadow_advisories = []
        trade_review = {}
        try:
            from scripts.pipeline.shadow_trade import get_status as get_shadow_status
            shadow_status = get_shadow_status()
            shadow_advisories = shadow_status.get("advisory_summary", {}).get("positions", [])
            if shadow_advisories:
                _logger.info(f"  影子盘 advisory 提示: {len(shadow_advisories)} 只")
        except Exception as e:
            _logger.warning(f"  影子盘 advisory 汇总失败: {e}")
        try:
            trade_review = load_trade_review(window=7, scope="cn_a_system")
        except Exception as e:
            _logger.warning(f"  交易归因汇总失败: {e}")

        _logger.info(">> 生成周报文件...")
        report_content, summary_stats = _build_weekly_report(
            vault, stats, core_pool_changes, trade_events, year, week_num, shadow_advisories, trade_review
        )

        review_dir = Path(vault.vault_path) / vault.weekly_review_dir
        review_dir.mkdir(parents=True, exist_ok=True)
        review_path = review_dir / f"{week_str}.md"
        with open(review_path, 'w', encoding='utf-8') as f:
            f.write(report_content)
        _logger.info(f"  已写入: {review_path.name}")

        discord_data = {
            "year": str(year),
            "week": week_str,
            "currency": "¥",
            "pnl_pct": 0.0,
            "pnl_abs": summary_stats["pnl_abs"],
            "win_rate": summary_stats["win_rate"],
            "trades": summary_stats["trade_count"],
            "profit_loss_ratio": summary_stats["profit_loss_ratio"],
            "position_changes": position_changes[:10] if position_changes else [
                {"action": "trade", "name": "（无）", "shares": 0, "price": 0, "currency": "¥"}
            ],
            "core_pool_changes": core_pool_changes,
            "next_week_plan": [
                "参照核心池评分报告",
                "遵守买入规则，不追高",
            ],
        }

        ok, err = send_weekly_report(discord_data)
        if ok:
            _logger.info(">> Discord 推送成功")
        else:
            _logger.warning(f">> Discord 推送失败: {err}")

        _logger.info(f"[WEEKLY] 周报完成 → {review_path.name}")

        update_pipeline_state(
            "weekly_review",
            "warning" if not ok else "success",
            {
                "week": week_str,
                "review_path": str(review_path),
                "discord_ok": ok,
                "discord_error": err,
                "core_pool_change_count": len(core_pool_changes),
                "trade_event_count": len(trade_events),
                "buy_count": buy_count,
                "sell_count": sell_count,
                "realized_pnl": realized_pnl,
                "shadow_advisory_count": len(shadow_advisories),
                "closed_trade_review_count": int(trade_review.get("closed_trade_count", 0) or 0),
            },
        )

        return {
            "week": week_str,
            "stats": stats,
            "summary": summary_stats,
            "review_path": str(review_path),
        }
    except Exception as e:
        update_pipeline_state(
            "weekly_review",
            "error",
            {
                "week": week_str,
                "error": str(e),
            },
        )
        raise


if __name__ == "__main__":
    result = run()
    print(f"\n周报已生成: {result['review_path']}")
    print(f"本周盈亏: ¥{result['summary']['pnl_abs']:+.2f}")
