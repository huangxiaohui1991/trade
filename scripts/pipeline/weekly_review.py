#!/usr/bin/env python3
"""
pipeline/weekly_review.py — 周报生成（周日 20:00 执行）

职责：
  1. 解析本周所有日志（parse_journal_dir）
  2. 统计：本周 P&L + 胜率 + 盈亏比
  3. 计算：核心池变化
  4. 输出到 vault/03-复盘/周/YYYY-W##.md
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
from datetime import datetime, timedelta
from pathlib import Path

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

os.environ["TQDM_DISABLE"] = "1"
warnings.filterwarnings("ignore")

from scripts.parser import parse_journal_dir
from scripts.utils.obsidian import ObsidianVault
from scripts.utils.discord_push import send_weekly_report
from scripts.utils.logger import get_logger

_logger = get_logger("pipeline.weekly_review")


def _safe_float(v, default=0.0):
    """安全转换为浮点数"""
    try:
        return float(v) if v else default
    except (ValueError, TypeError):
        return default


def _build_weekly_report(vault: ObsidianVault, stats: dict,
                          core_pool_changes: list, position_changes: list,
                          year: int, week_num: int) -> str:
    """生成周报 markdown 内容"""
    week_str = f"{year}-W{week_num:02d}"

    # 计算日期范围（本周周一到周五）
    now = datetime.now()
    monday = now - timedelta(days=now.weekday())
    friday = monday + timedelta(days=4)

    pnl_abs = stats.get("total_pnl", 0)
    win_days = stats.get("win_days", 0)
    loss_days = stats.get("loss_days", 0)
    total_days = win_days + loss_days
    win_rate = (win_days / total_days * 100) if total_days > 0 else 0

    # 盈亏比（简化：平均盈利日 vs 平均亏损日）
    journals = stats.get("journals", [])
    avg_win = 0.0
    avg_loss = 0.0
    win_count = 0
    loss_count = 0
    for j in journals:
        pnl = _safe_float(j.get("daily_pnl", 0))
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

    total_trades = stats.get("total_trades", 0)

    # 总收益率（需要手动填写资产）
    # pnl_pct 需要资产数据，这里留空让用户填写
    pnl_pct = 0.0  # 待计算

    lines = [
        f"# {week_str} 周复盘（{monday.strftime('%m/%d')} - {friday.strftime('%m/%d')}）",
        "",
        "---",
        "",
        "## 本周概览（自动统计）",
        "",
        "| 项目 | 数据 |",
        "|------|------|",
        f"| 周初资产 | ¥（手动填写） |",
        f"| 周末资产 | ¥（手动填写） |",
        f"| 本周盈亏 | ¥{pnl_abs:+,.2f}（自动统计） |",
        f"| 收益率 | %（需填写资产后计算） |",
        f"| 主动买入次数 | {total_trades} 次（自动统计） |",
        f"| 盈利天数 / 亏损天数 | {win_days} / {loss_days}（自动统计） |",
        f"| 胜率 | {win_rate:.0f}%（自动统计） |",
        f"| 盈亏比 | {profit_loss_ratio:.2f} |",
        "",
        "> 周初/周末资产需手动填写（券商APP截图），其余由系统从日志自动统计。",
        "",
        "---",
        "",
        "## 本周交易明细（自动从日志提取）",
        "",
        "| 日期 | 股票 | 操作 | 价格 | 数量 | 盈亏 | 备注 |",
        "|------|------|------|------|------|------|------|",
    ]

    # 从日志提取交易
    has_trades = False
    for j in journals:
        if _safe_float(j.get("trades", 0)) > 0:
            has_trades = True
            date = j.get("_date", "")
            pnl = j.get("daily_pnl", 0)
            lines.append(f"| {date} | — | — | — | — | ¥{pnl:+,} | 详见日志 |")
    if not has_trades:
        lines.append("| — | — | — | — | — | — | 本周无交易 |")

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
    try:
        vault_core = vault.read_core_pool()
        for row in vault_core:
            name = str(row.get("股票", "")).strip()
            code = str(row.get("代码", "")).strip()
            if name and name not in ["", "—"]:
                score = float(row.get("四维总分", row.get("总分", 0)))
                state = "✅ 可操作" if score >= 5 else "❌ 规避"
                lines.append(f"| {name} | {code} | {'关注买入' if score >= 7 else '观察'} | {state} |")
    except Exception:
        pass
    lines.extend(["", "### 下周改进点", "", "> "])

    return "\n".join(lines), {
        "pnl_abs": pnl_abs,
        "win_rate": win_rate,
        "profit_loss_ratio": profit_loss_ratio,
        "total_trades": total_trades,
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

    vault = ObsidianVault()
    journal_dir = os.path.join(vault.vault_path, vault.journal_dir)

    # 1. 统计本周数据
    _logger.info(">> 解析本周日志...")
    stats = parse_journal_dir(journal_dir, days=7)

    win_days = stats.get("win_days", 0)
    loss_days = stats.get("loss_days", 0)
    total_days = win_days + loss_days
    win_rate = (win_days / total_days * 100) if total_days > 0 else 0

    _logger.info(
        f"  本周交易日: {stats.get('count', 0)} | "
        f"盈利{win_days}/亏损{loss_days} | "
        f"胜率{win_rate:.0f}% | "
        f"总盈亏: ¥{stats.get('total_pnl', 0):+.2f} | "
        f"交易次数: {stats.get('total_trades', 0)}"
    )

    # 2. 核心池变化（从评分报告目录对比）
    _logger.info(">> 检查核心池变化...")
    core_pool_changes = []
    try:
        reports_dir = Path(vault.vault_path) / "04-选股" / "筛选结果"
        if reports_dir.exists():
            # 找最近两份评分报告
            reports = sorted(reports_dir.glob("核心池_评分报告_*.md"), key=lambda p: p.stat().st_mtime)
            if len(reports) >= 2:
                prev_report = reports[-2]  # 上周的
                curr_report = reports[-1]  # 本周的

                from scripts.parser import parse_md_table
                prev_data = parse_md_table(open(prev_report, encoding='utf-8').read())
                curr_data = parse_md_table(open(curr_report, encoding='utf-8').read())

                prev_scores = {}
                curr_scores = {}

                for t in prev_data:
                    for row in t.get("rows", []):
                        code = str(row.get("代码", "")).strip()
                        try:
                            prev_scores[code] = float(row.get("总分", 0))
                        except (ValueError, TypeError):
                            pass

                for t in curr_data:
                    for row in t.get("rows", []):
                        code = str(row.get("代码", "")).strip()
                        try:
                            curr_scores[code] = float(row.get("总分", 0))
                        except (ValueError, TypeError):
                            pass

                all_codes = set(list(prev_scores.keys()) + list(curr_scores.keys()))
                for code in all_codes:
                    old = prev_scores.get(code, 0)
                    new = curr_scores.get(code, 0)
                    if old != new:
                        reason = []
                        if new < old:
                            reason.append("评分下降")
                        else:
                            reason.append("评分上升")
                        core_pool_changes.append({
                            "name": code,  # name 需从 stocks.yaml 映射
                            "old_score": old,
                            "new_score": new,
                            "reason": ", ".join(reason),
                        })
    except Exception as e:
        _logger.warning(f"  核心池变化检测失败: {e}")

    # 3. 持仓变化（从日志提取）
    position_changes = []
    journals = stats.get("journals", [])
    for j in journals:
        # 简化：交易次数 > 0 的日子记录为有一次操作
        if _safe_float(j.get("trades", 0)) > 0:
            position_changes.append({
                "action": "trade",
                "date": j.get("_date", ""),
            })

    # 4. 生成周报
    _logger.info(">> 生成周报文件...")
    report_content, summary_stats = _build_weekly_report(
        vault, stats, core_pool_changes, position_changes, year, week_num
    )

    # 写文件
    review_dir = Path(vault.vault_path) / "data" / "03-复盘" / "周"
    review_dir.mkdir(parents=True, exist_ok=True)
    review_path = review_dir / f"{week_str}.md"
    with open(review_path, 'w', encoding='utf-8') as f:
        f.write(report_content)
    _logger.info(f"  已写入: {review_path.name}")

    # 5. Discord 推送
    weekday_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
    weekday = weekday_names[datetime.now().weekday()]

    discord_data = {
        "year": str(year),
        "week": week_str,
        "currency": "¥",
        "pnl_pct": 0.0,  # 待填资产后计算
        "pnl_abs": summary_stats["pnl_abs"],
        "win_rate": summary_stats["win_rate"],
        "trades": summary_stats["total_trades"],
        "profit_loss_ratio": summary_stats["profit_loss_ratio"],
        "position_changes": [
            {"action": "trade", "name": "（见日志）", "shares": 0, "price": 0, "currency": "¥"}
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

    return {
        "week": week_str,
        "stats": stats,
        "summary": summary_stats,
        "review_path": str(review_path),
    }


if __name__ == "__main__":
    result = run()
    print(f"\n周报已生成: {result['review_path']}")
    print(f"本周盈亏: ¥{result['summary']['pnl_abs']:+.2f}")
