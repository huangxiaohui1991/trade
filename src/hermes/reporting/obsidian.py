"""
reporting/obsidian.py — Obsidian vault 投影

对齐 trade-vault/ 目录结构：
  01-状态/持仓/持仓概览.md     ← write_portfolio_status()
  01-状态/池子/核心池.md       ← write_core_pool()
  01-状态/池子/观察池.md       ← write_watch_pool()
  01-状态/账户/账户总览.md     ← write_account_overview()
  02-运行/日志/{date}.md       ← write_daily_log()
  02-运行/当日输出/评分_{date}.md ← write_scoring_report()
  04-决策/今日决策.md          ← write_today_decision()
  04-决策/筛选结果/*.md        ← write_screening_result()

所有写入都是从投影表/event_log 生成，可删可重建。
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

from hermes.platform.events import EventStore

_logger = logging.getLogger(__name__)


class ObsidianProjector:
    """从投影表生成 Obsidian markdown 文件。"""

    def __init__(
        self,
        event_store: EventStore,
        conn: sqlite3.Connection,
        vault_path: Optional[str] = None,
    ):
        self._events = event_store
        self._conn = conn
        self._vault = Path(vault_path) if vault_path else None

    def _write(self, relative_path: str, content: str) -> Optional[str]:
        if not self._vault:
            _logger.debug("[obsidian] vault 路径未配置，跳过写入: %s", relative_path)
            return None
        full = self._vault / relative_path
        full.parent.mkdir(parents=True, exist_ok=True)
        full.write_text(content, encoding="utf-8")
        return str(full)

    def _now(self) -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M")

    def _today(self) -> str:
        return datetime.now().strftime("%Y-%m-%d")

    # ------------------------------------------------------------------
    # 01-状态/持仓/持仓概览.md
    # ------------------------------------------------------------------

    def write_portfolio_status(self) -> str:
        """从 projection_positions 生成持仓概览。"""
        rows = self._conn.execute(
            "SELECT * FROM projection_positions ORDER BY entry_date"
        ).fetchall()

        now = self._now()
        lines = [
            "---",
            f"updated_at: {now}",
            "type: portfolio_overview",
            "tags: [持仓, 状态, 自动更新]",
            "---",
            "",
            "# 持仓概览",
            "",
        ]

        if not rows:
            lines.append("当前无持仓。")
        else:
            total_cost = sum(r["avg_cost_cents"] * r["shares"] for r in rows) / 100
            total_market = sum((r["current_price_cents"] or r["avg_cost_cents"]) * r["shares"] for r in rows) / 100
            total_pnl = total_market - total_cost

            lines.extend([
                f"持仓 **{len(rows)}** 只 | 总成本 ¥{total_cost:,.0f} | 总市值 ¥{total_market:,.0f} | 浮动盈亏 ¥{total_pnl:+,.0f}",
                "",
                "| 代码 | 名称 | 风格 | 股数 | 成本 | 现价 | 盈亏 | 入场日 |",
                "|------|------|------|------|------|------|------|--------|",
            ])
            for r in rows:
                cost = r["avg_cost_cents"] / 100
                price = (r["current_price_cents"] or r["avg_cost_cents"]) / 100
                pnl = (price - cost) * r["shares"]
                lines.append(
                    f"| {r['code']} | {r['name']} | {r['style']} "
                    f"| {r['shares']} | ¥{cost:.2f} | ¥{price:.2f} "
                    f"| ¥{pnl:+,.0f} | {r['entry_date']} |"
                )

        content = "\n".join(lines) + "\n"
        self._write("01-状态/持仓/持仓概览.md", content)
        return content

    # ------------------------------------------------------------------
    # 01-状态/池子/核心池.md + 观察池.md
    # ------------------------------------------------------------------

    def write_core_pool(self) -> str:
        """从 projection_candidate_pool 生成核心池页。"""
        rows = self._conn.execute(
            "SELECT * FROM projection_candidate_pool WHERE pool_tier = 'core' ORDER BY score DESC"
        ).fetchall()

        now = self._now()
        lines = [
            "---",
            f"date: {self._today()}",
            "type: watchlist_core",
            "tags: [核心池, 选股]",
            f"updated_at: {now}",
            "---",
            "",
            "# 核心池（结构化投影）",
            "",
            f"> 更新时间：{now}",
            "",
            "| # | 股票 | 代码 | 四维总分 | 加入日 | 连续天数 | 备注 |",
            "|---|------|------|---------|--------|----------|------|",
        ]

        for i, r in enumerate(rows, 1):
            score = r["score"] or 0
            emoji = "✅" if score >= 7 else ("🟡" if score >= 5 else "❌")
            lines.append(
                f"| {i} | {r['name'] or ''} | {r['code']} "
                f"| {emoji} **{score:.1f}** "
                f"| {(r['added_at'] or '')[:10]} | {r['streak_days'] or 0} | {r['note'] or ''} |"
            )
        if not rows:
            lines.append("| — | — | — | — | — | — | 暂无 |")

        content = "\n".join(lines) + "\n"
        self._write("01-状态/池子/核心池.md", content)
        return content

    def write_watch_pool(self) -> str:
        """从 projection_candidate_pool 生成观察池页。"""
        rows = self._conn.execute(
            "SELECT * FROM projection_candidate_pool WHERE pool_tier = 'watch' ORDER BY score DESC"
        ).fetchall()

        now = self._now()
        lines = [
            "---",
            f"date: {self._today()}",
            "type: watchlist_observe",
            "tags: [观察池, 选股]",
            f"updated_at: {now}",
            "---",
            "",
            "# 观察池（结构化投影）",
            "",
            f"> 更新时间：{now}",
            "",
            "| # | 股票 | 代码 | 四维总分 | 加入日 | 备注 |",
            "|---|------|------|---------|--------|------|",
        ]

        for i, r in enumerate(rows, 1):
            score = r["score"] or 0
            lines.append(
                f"| {i} | {r['name'] or ''} | {r['code']} "
                f"| **{score:.1f}** | {(r['added_at'] or '')[:10]} | {r['note'] or ''} |"
            )
        if not rows:
            lines.append("| — | — | — | — | — | 暂无 |")

        content = "\n".join(lines) + "\n"
        self._write("01-状态/池子/观察池.md", content)
        return content

    # 兼容旧接口
    def write_pool_status(self) -> str:
        self.write_core_pool()
        return self.write_watch_pool()

    # ------------------------------------------------------------------
    # 01-状态/账户/账户总览.md
    # ------------------------------------------------------------------

    def write_account_overview(self, capital: float = 0, cash: float = 0) -> str:
        """生成账户总览。"""
        rows = self._conn.execute(
            "SELECT * FROM projection_positions ORDER BY entry_date"
        ).fetchall()

        now = self._now()
        total_market = sum((r["current_price_cents"] or r["avg_cost_cents"]) * r["shares"] for r in rows) / 100
        total_asset = cash + total_market

        lines = [
            "---",
            f"updated_at: {now}",
            "type: account_overview",
            "tags: [账户, 状态, 自动更新]",
            "---",
            "",
            "# 账户总览",
            "",
            "| 项目 | 金额 |",
            "|------|------|",
            f"| 现金 | ¥{cash:,.0f} |",
            f"| 持仓市值 | ¥{total_market:,.0f} |",
            f"| 总资产 | ¥{total_asset:,.0f} |",
            f"| 持仓数 | {len(rows)} 只 |",
        ]

        content = "\n".join(lines) + "\n"
        self._write("01-状态/账户/账户总览.md", content)
        return content

    # ------------------------------------------------------------------
    # 02-运行/日志/{date}.md
    # ------------------------------------------------------------------

    def write_daily_log(self, run_id: str, report: str) -> str:
        """写入/追加今日日志。"""
        today = self._today()
        path = f"02-运行/日志/{today}.md"

        # 如果文件已存在，追加内容
        if self._vault:
            full = self._vault / path
            if full.exists():
                existing = full.read_text(encoding="utf-8")
                content = existing.rstrip() + "\n\n" + report.strip() + "\n"
                self._write(path, content)
                return content

        # 新建
        now = self._now()
        lines = [
            "---",
            f"date: {today}",
            f"updated_at: {now}",
            f"run_id: {run_id}",
            "type: journal",
            "tags: [交易日志, 自动更新]",
            "---",
            "",
            f"# {today} 交易日志",
            "",
            report,
        ]
        content = "\n".join(lines) + "\n"
        self._write(path, content)
        return content

    # ------------------------------------------------------------------
    # 02-运行/当日输出/评分_{date}.md
    # ------------------------------------------------------------------

    def write_scoring_report(self, run_id: str, scores: list[dict]) -> str:
        """写入评分报告。"""
        today = self._today()
        now = self._now()
        ts = datetime.now().strftime("%H%M%S")

        lines = [
            "---",
            f"date: {today}",
            f"updated_at: {now}",
            f"run_id: {run_id}",
            "type: scoring_report",
            "tags: [评分, 核心池, 自动更新]",
            "---",
            "",
            f"# 核心池评分 — {today}",
            "",
            "| # | 名称 | 代码 | 总分 | 技术 | 基本面 | 资金 | 舆情 | 风格 | 状态 |",
            "|---|------|------|------|------|--------|------|------|------|------|",
        ]

        for i, s in enumerate(scores, 1):
            total = float(s.get("total_score", s.get("total", 0)) or 0)
            veto = s.get("veto_triggered", False)
            status = "❌" if veto else ("✅" if total >= 7 else ("🟡" if total >= 5 else "❌"))
            lines.append(
                f"| {i} | {s.get('name', '')} | {s.get('code', '')} "
                f"| **{total:.1f}** "
                f"| {float(s.get('technical_score', 0) or 0):.1f} "
                f"| {float(s.get('fundamental_score', 0) or 0):.1f} "
                f"| {float(s.get('flow_score', 0) or 0):.1f} "
                f"| {float(s.get('sentiment_score', 0) or 0):.1f} "
                f"| {s.get('style', '')} | {status} |"
            )

        content = "\n".join(lines) + "\n"
        # 同时写到两个位置：当日输出 + 筛选结果
        self._write(f"02-运行/当日输出/评分_{today}.md", content)
        self._write(f"04-决策/筛选结果/核心池_评分报告_{today}_{ts}.md", content)
        return content

    # ------------------------------------------------------------------
    # 04-决策/今日决策.md
    # ------------------------------------------------------------------

    def write_today_decision(
        self,
        market_signal: str = "",
        multiplier: float = 0.0,
        can_buy: bool = False,
        weekly_buys: int = 0,
        holding_count: int = 0,
        exposure_pct: float = 0.0,
        reasons: list[str] = None,
    ) -> str:
        """生成今日决策页。"""
        now = self._now()
        reasons = reasons or []

        if market_signal in ("RED", "CLEAR"):
            action = "NO_TRADE"
        elif not can_buy:
            action = "NO_TRADE"
        elif market_signal == "YELLOW":
            action = "REDUCED_BUY"
        else:
            action = "BUY_ALLOWED"

        lines = [
            "---",
            f"updated_at: {now}",
            "type: today_decision",
            "tags: [决策, 自动更新]",
            "---",
            "",
            "# 今日决策",
            "",
            "## 决策摘要",
            "",
            "| 项目 | 数值 |",
            "|------|------|",
            f"| 决策动作 | {action} |",
            f"| 市场信号 | {market_signal or '—'} |",
            f"| 仓位系数 | {multiplier:.2f} |",
            f"| 当前仓位 | {exposure_pct:.1%} |",
            f"| 本周买入次数 | {weekly_buys} |",
            f"| 当前持仓数 | {holding_count} |",
            "",
            "## 原因说明",
            "",
        ]
        if reasons:
            lines.extend([f"- {r}" for r in reasons])
        else:
            lines.append("- 无")

        content = "\n".join(lines) + "\n"
        self._write("04-决策/今日决策.md", content)
        return content

    # ------------------------------------------------------------------
    # 全量刷新
    # ------------------------------------------------------------------

    def refresh_all(self, capital: float = 0, cash: float = 0) -> dict:
        """刷新所有 vault 投影页。"""
        results = {}
        results["portfolio"] = self.write_portfolio_status()
        results["core_pool"] = self.write_core_pool()
        results["watch_pool"] = self.write_watch_pool()
        results["account"] = self.write_account_overview(capital, cash)
        return results
