#!/usr/bin/env python3
"""
Obsidian Vault 文件读写工具
提供持仓、核心池、日志等文件的读写接口
"""

import os
import shutil
import re
from datetime import datetime
from pathlib import Path
from typing import Optional, Any, Union

# 导入 parser 模块的解析函数
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from scripts.utils.common import _safe_float
from scripts.utils.exceptions import VaultError
from scripts.utils.parser import parse_frontmatter, parse_md_table, parse_portfolio as parse_portfolio_file
from scripts.engine.scorer import split_veto_signals
from scripts.state.reason_codes import VETO_LABEL_MAP
from scripts.utils.config_loader import get_paths


VAULT_LAYOUT: dict[str, str] = {
    "system_dir": "00-系统",
    "state_dir": "01-状态",
    "run_dir": "02-运行",
    "analysis_dir": "03-分析",
    "decision_dir": "04-决策",
    "portfolio_dir": "01-状态/持仓",
    "account_dir": "01-状态/账户",
    "pool_dir": "01-状态/池子",
    "journal_dir": "02-运行/日志",
    "paper_trade_dir": "02-运行/模拟盘",
    "daily_output_dir": "02-运行/当日输出",
    "weekly_review_dir": "03-分析/周复盘",
    "monthly_review_dir": "03-分析/月复盘",
    "screening_results_dir": "04-决策/筛选结果",
    "candidate_pool_dir": "04-决策/候选池",
    "stock_explain_dir": "04-决策/个股解释",
    "portfolio_path": "01-状态/持仓/portfolio.md",
    "portfolio_overview_path": "01-状态/持仓/持仓概览.md",
    "account_overview_path": "01-状态/账户/账户总览.md",
    "core_pool_path": "01-状态/池子/核心池.md",
    "watch_pool_path": "01-状态/池子/观察池.md",
    "today_decision_path": "04-决策/今日决策.md",
    "signal_snapshot_dir": "02-运行/信号快照",
}


def default_vault_path(project_root: Optional[Path] = None) -> str:
    if project_root is None:
        project_root = Path(__file__).resolve().parent.parent.parent
    env_path = os.environ.get("AStockVault", "").strip()
    if env_path:
        return str(Path(env_path).expanduser().resolve())
    config_path = str(get_paths().get("vault_path", "") or "").strip()
    if config_path:
        configured_path = Path(config_path).expanduser()
        if not configured_path.is_absolute():
            configured_path = project_root / configured_path
        return str(configured_path.resolve())
    local_vault_path = project_root / "trade-vault"
    if local_vault_path.exists():
        return str(local_vault_path.resolve())
    sibling_path = project_root.parent / "trade-vault"
    if sibling_path.exists():
        return str(sibling_path.resolve())
    return str(project_root.resolve())


class ObsidianVault:
    """Obsidian vault 文件读写工具"""

    def __init__(self, vault_path: Optional[str] = None):
        """
        初始化 Obsidian vault

        Args:
            vault_path: vault 根目录，默认按以下优先级解析：
                1. 显式传入 vault_path
                2. 环境变量 AStockVault
                3. config/paths.yaml 的 vault_path
                4. 当前仓库内 trade-vault
                5. 当前仓库同级 trade-vault
                6. 当前仓库根目录
        """
        if vault_path is None:
            project_root = Path(__file__).resolve().parent.parent.parent
            vault_path = default_vault_path(project_root)
        self.vault_path = os.path.abspath(vault_path)

        for key, value in VAULT_LAYOUT.items():
            setattr(self, key, value)

        # 自动创建所有必要的子目录
        self._ensure_directories()

    def _ensure_directories(self) -> None:
        """确保 VAULT_LAYOUT 中定义的所有目录都存在"""
        for key, value in VAULT_LAYOUT.items():
            if key.endswith("_dir"):
                # 目录直接创建
                full_path = self._full_path(value)
                os.makedirs(full_path, exist_ok=True)
            elif key.endswith("_path"):
                # 文件路径创建其父目录
                full_path = self._full_path(value)
                dir_path = os.path.dirname(full_path)
                if dir_path:
                    os.makedirs(dir_path, exist_ok=True)

    def _full_path(self, relative_path: str) -> str:
        """将相对路径转换为绝对路径"""
        return os.path.join(self.vault_path, relative_path)

    def _backup(self, file_path: str) -> None:
        """备份原文件为 .bak"""
        if os.path.exists(file_path):
            bak_path = file_path + ".bak"
            shutil.copy2(file_path, bak_path)

    def read(self, relative_path: str) -> str:
        """
        读取文件内容

        Args:
            relative_path: 相对于 vault 根目录的路径

        Returns:
            文件内容字符串
        """
        full_path = self._full_path(relative_path)
        with open(full_path, 'r', encoding='utf-8') as f:
            return f.read()

    def write(self, relative_path: str, content: str, append: bool = False) -> None:
        """
        写文件（append=False 时自动备份原文件）

        Args:
            relative_path: 相对于 vault 根目录的路径
            content: 文件内容
            append: 若为 True，则追加到文件末尾（不备份）；若为 False，则覆盖并备份
        """
        full_path = self._full_path(relative_path)

        # 确保目录存在
        os.makedirs(os.path.dirname(full_path), exist_ok=True)

        if append:
            with open(full_path, 'a', encoding='utf-8') as f:
                f.write(content)
        else:
            self._backup(full_path)
            with open(full_path, 'w', encoding='utf-8') as f:
                f.write(content)

    @staticmethod
    def _fmt_currency(value: Optional[Union[float, int, str]]) -> str:
        try:
            return f"¥{float(value or 0):,.2f}"
        except (TypeError, ValueError):
            return "¥0.00"

    @staticmethod
    def _fmt_pct(value) -> str:
        try:
            return f"{float(value or 0) * 100:.1f}%"
        except (TypeError, ValueError):
            return "0.0%"

    @staticmethod
    def _fmt_count(value: Optional[Union[float, int, str]]) -> str:
        try:
            return str(int(value or 0))
        except (TypeError, ValueError):
            return "0"

    @staticmethod
    def _display_source(source: str) -> str:
        mapping = {
            "bootstrap:portfolio": "portfolio.md",
            "broker:mx_moni": "MX 模拟盘",
            "bootstrap:shadow": "初始化占位",
        }
        source = str(source or "").strip()
        return mapping.get(source, source or "—")

    @staticmethod
    def _snapshot_balance(snapshot: dict, scope: str = "") -> dict[str, Any]:
        if not isinstance(snapshot, dict):
            return {}
        balances = snapshot.get("balances", [])
        if not isinstance(balances, list):
            return {}
        for item in balances:
            if not isinstance(item, dict):
                continue
            if scope and str(item.get("scope", "")).strip() != scope:
                continue
            return item
        return {}

    @staticmethod
    def _snapshot_health(balance: dict) -> tuple[str, list[str]]:
        metadata = balance.get("metadata", {}) if isinstance(balance, dict) else {}
        if not isinstance(metadata, dict):
            metadata = {}
        issues = []
        for key in ("balance", "positions"):
            payload = metadata.get(key)
            if not isinstance(payload, dict):
                continue
            success = payload.get("success")
            message = str(payload.get("message", "")).strip()
            code = payload.get("code", payload.get("status", ""))
            if success is False or message:
                suffix = f" (code={code})" if code not in ("", None) else ""
                issues.append(f"{key}: {message or '请求失败'}{suffix}")
        return ("降级" if issues else "正常"), issues

    def _render_snapshot_row(self, label: str, snapshot: dict, scope: str) -> tuple[str, list[str]]:
        snapshot = snapshot if isinstance(snapshot, dict) else {}
        summary = snapshot.get("summary", {}) if isinstance(snapshot.get("summary", {}), dict) else {}
        balance = self._snapshot_balance(snapshot, scope=scope)
        status, issues = self._snapshot_health(balance)
        as_of_date = str(snapshot.get("as_of_date", balance.get("as_of_date", ""))).strip() or "—"
        market_value = balance.get("total_market_value", 0.0)
        total_capital = summary.get("total_capital", balance.get("total_capital", 0.0))
        row = (
            f"| {label} | {as_of_date} | {self._display_source(balance.get('source', ''))} | "
            f"{status} | {self._fmt_count(summary.get('holding_count', 0))} | "
            f"{self._fmt_pct(summary.get('current_exposure', balance.get('exposure', 0.0)))} | "
            f"{self._fmt_currency(summary.get('cash_value', balance.get('cash_value', 0.0)))} | "
            f"{self._fmt_currency(market_value)} | "
            f"{self._fmt_currency(total_capital)} |"
        )
        return row, issues

    def _render_positions_table(self, positions: list[dict], scope: str) -> list[str]:
        rows = [
            "| 股票 | 代码 | 持仓 | 成本 | 现价 | 市值 | 状态 |",
            "|------|------|------|------|------|------|------|",
        ]
        scoped_positions = []
        for item in positions if isinstance(positions, list) else []:
            if not isinstance(item, dict):
                continue
            if scope and str(item.get("scope", "")).strip() != scope:
                continue
            scoped_positions.append(item)
        scoped_positions.sort(key=lambda item: float(item.get("market_value", 0.0) or 0.0), reverse=True)
        if not scoped_positions:
            rows.append("| — | — | — | — | — | — | — |")
            return rows
        for item in scoped_positions:
            rows.append(
                f"| {item.get('name', '') or '—'} | {item.get('code', '') or '—'} | "
                f"{self._fmt_count(item.get('shares', 0))} | "
                f"{self._fmt_currency(item.get('avg_cost', 0.0))} | "
                f"{self._fmt_currency(item.get('current_price', 0.0))} | "
                f"{self._fmt_currency(item.get('market_value', 0.0))} | "
                f"{item.get('status', '') or '—'} |"
            )
        return rows

    def render_account_overview(self, primary_snapshot: dict, paper_snapshot: Optional[dict] = None) -> str:
        """渲染账户总览 markdown。

        实盘 = cn_a_system（A 股） + hk_legacy（港股）合并计算。
        """
        primary_snapshot = primary_snapshot or {}
        paper_snapshot = paper_snapshot or {}

        # 合并 cn_a_system + hk_legacy 为实盘总览
        balances = primary_snapshot.get("balances", []) if isinstance(primary_snapshot, dict) else []
        cn_balance = next((b for b in balances if b.get("scope") == "cn_a_system"), {})
        hk_balance = next((b for b in balances if b.get("scope") == "hk_legacy"), {})
        real_balance = {
            "scope": "实盘合并",
            "cash_value": cn_balance.get("cash_value", 0.0) + hk_balance.get("cash_value", 0.0),
            "total_market_value": cn_balance.get("total_market_value", 0.0) + hk_balance.get("total_market_value", 0.0),
            "total_capital": cn_balance.get("total_capital", 0.0) + hk_balance.get("total_capital", 0.0),
            "exposure": 0.0,  # 由 _render_snapshot_row 重新计算
            "source": cn_balance.get("source", ""),
            "as_of_date": max(
                str(cn_balance.get("as_of_date", "") or ""),
                str(hk_balance.get("as_of_date", "") or ""),
            ) or "—",
        }
        # 合并后暴露度 = 总市值 / 总资产
        if real_balance["total_capital"] > 0:
            real_balance["exposure"] = round(real_balance["total_market_value"] / real_balance["total_capital"], 4)

        overview_rows = cn_balance.get("metadata", {}).get("account_overview", []) if isinstance(cn_balance, dict) else []

        all_positions = primary_snapshot.get("positions", []) if isinstance(primary_snapshot, dict) else []
        # 实盘持仓 = cn_a_system + hk_legacy
        real_positions = [p for p in all_positions if p.get("scope") in ("cn_a_system", "hk_legacy")]
        paper_positions = [p for p in all_positions if p.get("scope") == "paper_mx"]

        # 传入合成 snapshot（含 balance），避免 _snapshot_balance 取错 scope
        real_snapshot = {
            "summary": {
                "total_capital": real_balance["total_capital"],
                "cash_value": real_balance["cash_value"],
                "total_market_value": real_balance["total_market_value"],
                "current_exposure": real_balance["exposure"],
                "holding_count": len([p for p in real_positions if int(p.get("shares", 0) or 0) > 0]),
            },
            "as_of_date": real_balance["as_of_date"],
            "balances": [real_balance],
        }
        real_row, real_issues = self._render_snapshot_row("实盘", real_snapshot, scope="")
        paper_row, paper_issues = self._render_snapshot_row("模拟盘", paper_snapshot, "paper_mx")

        # 合并 cn_a + hk 的 metadata 补充摘录
        hk_meta = hk_balance.get("metadata", {}) if isinstance(hk_balance, dict) else {}
        hk_rows = hk_meta.get("hk_legacy_holdings", [])
        if hk_rows and isinstance(overview_rows, list):
            for row in overview_rows:
                if row.get("项目", "").startswith("港股持仓"):
                    row["金额"] = f"¥{hk_balance.get('total_market_value', 0):.2f}"
                    if hk_rows:
                        row["说明"] = f"{hk_rows[0].get('持有股数', 0)}股 × 成本¥{hk_rows[0].get('平均成本', 0)}"
        # 更新总资产行
        for row in overview_rows:
            if row.get("项目", "").startswith("账户总资产"):
                row["金额"] = f"约¥{real_balance['total_capital']:.2f}"
                row["说明"] = f"现金{real_balance['cash_value']:.0f} + 港股市值{real_balance['total_market_value']:.2f}"

        lines = [
            "---",
            f"updated_at: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "type: account_overview",
            "tags: [账户, 状态, 自动更新]",
            "---",
            "",
            "# 账户总览",
            "",
            "## 快照总览",
            "",
            "| 账户 | 快照日期 | 数据来源 | 数据状态 | 持仓数 | 仓位 | 现金 | 持仓市值 | 总资产 |",
            "|------|----------|----------|----------|--------|------|------|----------|--------|",
            real_row,
            paper_row,
            "",
            "## 数据提示",
            "",
        ]

        if real_issues or paper_issues:
            if real_issues:
                lines.extend([f"- 实盘: {item}" for item in real_issues])
            if paper_issues:
                lines.extend([f"- 模拟盘: {item}" for item in paper_issues])
        else:
            lines.append("- 当前两路账户快照均正常。")

        if overview_rows:
            lines.extend([
                "",
                "## 实盘补充摘录",
                "",
                "| 项目 | 金额 | 说明 |",
                "|------|------|------|",
            ])
            for row in overview_rows:
                if not isinstance(row, dict):
                    continue
                lines.append(
                    f"| {row.get('项目', '') or '—'} | {row.get('金额', '') or '—'} | {row.get('说明', '') or '—'} |"
                )

        lines.extend([
            "",
            "## 实盘持仓",
            "",
        ])
        lines.extend(self._render_positions_table(real_positions, scope=""))
        lines.extend([
            "",
            "## 模拟盘持仓",
            "",
        ])
        lines.extend(self._render_positions_table(paper_positions, scope="paper_mx"))
        lines.extend([
            "",
            "## 备注",
            "",
            "- 本页由结构化账本自动投影生成。",
            "- 运行 `trade status today` 会刷新本页和 `今日决策.md`。",
        ])
        return "\n".join(lines)

    def write_account_overview(self, primary_snapshot: dict, paper_snapshot: Optional[dict] = None) -> str:
        """写入账户总览。"""
        self.write(self.account_overview_path, self.render_account_overview(primary_snapshot, paper_snapshot))
        return self._full_path(self.account_overview_path)

    def render_today_decision(self, today_decision: dict) -> str:
        """渲染今日决策 markdown。"""
        today_decision = today_decision or {}
        risk = today_decision.get("risk", {}) if isinstance(today_decision.get("risk", {}), dict) else {}
        portfolio_risk = (
            today_decision.get("portfolio_risk", {})
            if isinstance(today_decision.get("portfolio_risk", {}), dict)
            else {}
        )
        reasons = [str(item).strip() for item in (today_decision.get("reasons", []) or []) if str(item).strip()]
        reason_codes = [str(item).strip() for item in (today_decision.get("reason_codes", []) or []) if str(item).strip()]

        lines = [
            "---",
            f"updated_at: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
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
            f"| 决策动作 | {today_decision.get('action', today_decision.get('decision', '')) or '—'} |",
            f"| 市场信号 | {today_decision.get('market_signal', '') or '—'} |",
            f"| 仓位系数 | {float(today_decision.get('market_multiplier', 0.0) or 0.0):.2f} |",
            f"| 当前仓位 | {self._fmt_pct(today_decision.get('current_exposure', 0.0))} |",
            f"| 本周买入次数 | {int(today_decision.get('weekly_buys', 0) or 0)} |",
            f"| 当前持仓数 | {int(today_decision.get('holding_count', 0) or 0)} |",
            "",
            "## 风控状态",
            "",
            "| 项目 | 状态 |",
            "|------|------|",
            f"| 买入风控 | {'允许' if risk.get('can_buy', False) else '阻断'} |",
            f"| 组合风控 | {portfolio_risk.get('state', 'ok') or 'ok'} |",
            "",
            "## 原因说明",
            "",
        ]
        if reasons:
            lines.extend([f"- {item}" for item in reasons])
        else:
            lines.append("- 无")
        lines.extend([
            "",
            "## 原因代码",
            "",
        ])
        if reason_codes:
            lines.extend([f"- `{item}`" for item in reason_codes])
        else:
            lines.append("- 无")
        return "\n".join(lines)

    def write_today_decision(self, today_decision: dict) -> str:
        """写入今日决策。"""
        self.write(self.today_decision_path, self.render_today_decision(today_decision))
        return self._full_path(self.today_decision_path)

    def sync_portfolio_state(self) -> dict:
        """将 portfolio.md 当前内容同步到结构化账本。"""
        from scripts.state import sync_portfolio_state
        return sync_portfolio_state()

    def read_portfolio(self) -> dict:
        """
        读取并解析 portfolio.md

        Returns:
            包含 meta 和 holdings 的字典
        """
        full_path = self._full_path(self.portfolio_path)
        return parse_portfolio_file(full_path)

    def update_portfolio(self, updates: dict) -> None:
        """
        更新 portfolio.md

        Args:
            updates: 要更新的字段，支持:
                - meta: dict, 更新 frontmatter 字段
                - holdings: list, 更新持仓明细表格
        """
        full_path = self._full_path(self.portfolio_path)
        content = self.read(self.portfolio_path)

        # 解析现有内容
        frontmatter = parse_frontmatter(content)
        tables = parse_md_table(content)

        # 更新 frontmatter
        if 'meta' in updates:
            frontmatter.update(updates['meta'])

        # 构建新的 frontmatter 字符串
        fm_lines = ["---"]
        for key, value in frontmatter.items():
            if isinstance(value, list):
                fm_lines.append(f"{key}: [{', '.join(str(v) for v in value)}]")
            else:
                fm_lines.append(f"{key}: {value}")
        fm_lines.append("---")

        new_frontmatter = "\n".join(fm_lines)

        # 更新表格（如果有的话）
        new_content = content
        if 'holdings' in updates and tables:
            # 更新第一个表格（A股持仓明细）
            headers = tables[0].get('headers', [])
            if headers:
                # 构建新的表格行
                new_table_lines = ["| " + " | ".join(headers) + " |",
                                   "| " + " | ".join(["---"] * len(headers)) + " |"]
                for row in updates['holdings']:
                    cells = [str(row.get(h, "")) for h in headers]
                    new_table_lines.append("| " + " | ".join(cells) + " |")

                # 找到表格在原文件中的位置并替换
                # 这是一个简化实现，实际应该更精确地定位表格
                table_str = "\n".join(new_table_lines)
                # 尝试找到 A股持仓明细 表格并替换
                pattern = r'(## A股持仓明细\n\n)\|.*\|.*\|\n\|[|\-\s]+\|\n(.*?)(?=\n##|\n#|$)'
                match = re.search(pattern, content, re.DOTALL)
                if match:
                    new_content = content[:match.start()] + \
                                  match.group(1) + table_str + \
                                  content[match.end():]

        # 合并 frontmatter 和剩余内容
        if new_content.startswith('---'):
            # 替换原有的 frontmatter
            end_marker = new_content.find('---', 4)
            if end_marker != -1:
                new_content = new_frontmatter + "\n" + new_content[end_marker + 4:]

        self.write(self.portfolio_path, new_content)
        self.sync_portfolio_state()

    def read_core_pool(self) -> list[dict]:
        """
        读取核心池.md

        Returns:
            核心池列表（字典列表）
        """
        full_path = self._full_path(self.core_pool_path)
        content = self.read(self.core_pool_path)

        tables = parse_md_table(content)
        if tables:
            return tables[0].get('rows', [])
        return []

    def _render_pool_table(self, entries: list[dict], bucket: str) -> str:
        """将结构化 pool entries 渲染成 markdown 表格。"""
        rows = [
            "| # | 股票 | 代码 | 四维总分 | 技术 | 基本面 | 资金 | 舆情 | 通过 | 备注 |",
            "|---|------|------|---------|------|--------|------|------|------|------|",
        ]
        for idx, entry in enumerate(entries, 1):
            hard_veto, warnings = split_veto_signals(entry.get("veto_signals", []))
            score = float(entry.get("total_score", 0) or 0)
            if hard_veto:
                status = "❌"
            elif score >= 7:
                status = "✅"
            elif score >= 5:
                status = "🟡"
            else:
                status = "❌"
            note = str(entry.get("note", "") or "").strip()
            if not note and warnings:
                w_labels = [VETO_LABEL_MAP.get(s, s) for s in warnings]
                note = f"预警:{','.join(w_labels)}"
            rows.append(
                f"| {idx} | {entry.get('name', '')} | {entry.get('code', '')} | "
                f"**{score:.1f}** | {float(entry.get('technical_score', 0) or 0):.1f} | "
                f"{float(entry.get('fundamental_score', 0) or 0):.1f} | "
                f"{float(entry.get('flow_score', 0) or 0):.1f} | "
                f"{float(entry.get('sentiment_score', 0) or 0):.1f} | {status} | {note or bucket} |"
            )
        if len(rows) == 2:
            rows.append("| — | — | — | — | — | — | — | — | 暂无 | 暂无 |")
        return "\n".join(rows)

    def sync_pool_projection(self, entries: list, metadata: Optional[dict] = None) -> dict:
        """
        同步核心池/观察池投影。

        entries 需要包含 bucket/core|watch|avoid，以及统一评分字段。
        """
        metadata = metadata or {}
        updated_at = metadata.get("updated_at") or datetime.now().strftime("%Y-%m-%d")
        source = metadata.get("source", "pool_snapshot")

        core_entries = [entry for entry in entries if str(entry.get("bucket", "")).strip() == "core"]
        watch_entries = [entry for entry in entries if str(entry.get("bucket", "")).strip() == "watch"]
        avoid_entries = [entry for entry in entries if str(entry.get("bucket", "")).strip() == "avoid"]

        core_content = "\n".join([
            "---",
            f"date: {updated_at}",
            "type: watchlist_core",
            "tags: [核心池, 选股]",
            f"updated_at: {updated_at}",
            "---",
            "",
            "# 核心池（结构化投影）",
            "",
            f"> 来源：{source}",
            "",
            self._render_pool_table(core_entries, "core"),
            "",
        ])
        watch_content = "\n".join([
            "---",
            f"date: {updated_at}",
            "type: watchlist_observe",
            "tags: [观察池, 选股]",
            f"updated_at: {updated_at}",
            "---",
            "",
            "# 观察池（结构化投影）",
            "",
            f"> 来源：{source}",
            "",
            f"## 当前观察池（{len(watch_entries)}只）",
            "",
            self._render_pool_table(watch_entries, "watch"),
            "",
            f"## 被淘汰（{len(avoid_entries)}只）",
            "",
            "| 股票 | 代码 | 总分 | 原因 |",
            "|------|------|------|------|",
        ])
        if avoid_entries:
            for entry in avoid_entries:
                hard_veto, warnings = split_veto_signals(entry.get("veto_signals", []))
                reason = str(entry.get("note", "") or "").strip()
                if not reason:
                    if hard_veto:
                        labels = [VETO_LABEL_MAP.get(s, s) for s in hard_veto]
                        reason = f"veto:{','.join(labels)}"
                    elif warnings:
                        labels = [VETO_LABEL_MAP.get(s, s) for s in warnings]
                        reason = f"预警:{','.join(labels)}"
                    else:
                        reason = "规避"
                watch_content += "\n" + f"| {entry.get('name', '')} | {entry.get('code', '')} | {float(entry.get('total_score', 0) or 0):.1f} | {reason} |"
        else:
            watch_content += "\n| — | — | — | 暂无 |"
        watch_content += "\n"

        self.write(self.core_pool_path, core_content)
        self.write(self.watch_pool_path, watch_content)
        return {
            "core_pool_path": self._full_path(self.core_pool_path),
            "watch_pool_path": self._full_path(self.watch_pool_path),
        }

    def update_core_pool_scores(self, scores: list) -> None:
        """
        更新核心池评分

        Args:
            scores: 评分列表，每项为 dict，包含 code（股票代码）和评分字段
                   例如: [{"code": "002487", "四维总分": 5, "基本面": 2, ...}]
        """
        full_path = self._full_path(self.core_pool_path)
        content = self.read(self.core_pool_path)

        # 解析表格
        tables = parse_md_table(content)
        if not tables:
            return

        # 建立代码到新评分的映射，兼容 code / 代码 两种字段
        score_map = {}
        for score in scores:
            code = str(score.get("code", score.get("代码", ""))).strip()
            if code:
                score_map[code] = score

        headers = tables[0].get("headers", [])
        rows = tables[0].get("rows", [])

        # 更新核心池主表，其他表格保持原样
        for row in rows:
            code = str(row.get("代码", "")).strip()
            if code not in score_map:
                continue

            new_score = score_map[code]
            total_score = _safe_float(new_score.get("total_score", row.get("四维总分", 0)))
            fundamental_score = _safe_float(new_score.get("fundamental_score", row.get("基本面", 0)))
            technical_score = _safe_float(new_score.get("technical_score", row.get("技术", 0)))
            flow_score = _safe_float(new_score.get("flow_score", row.get("主力", 0)))
            veto_signals = new_score.get("veto_signals", [])
            hard_veto, warning_signals = split_veto_signals(veto_signals)

            if hard_veto:
                suggestion = "❌"
                labels = [VETO_LABEL_MAP.get(s, s) for s in hard_veto]
                note = "veto:" + ",".join(labels)
            elif total_score >= 7:
                suggestion = "✅"
                note = "可买入"
            elif total_score >= 5:
                suggestion = "🟡"
                note = "观察"
            else:
                suggestion = "❌"
                note = "规避"

            if warning_signals and not hard_veto:
                w_labels = [VETO_LABEL_MAP.get(s, s) for s in warning_signals]
                note = f"预警:{','.join(w_labels)}"

            row["四维总分"] = f"{total_score:.1f}"
            if "基本面" in headers:
                row["基本面"] = f"{fundamental_score:.1f}"
            if "技术" in headers:
                row["技术"] = f"{technical_score:.1f}"
            if "主力" in headers:
                row["主力"] = f"{flow_score:.1f}"
            if "通过" in headers:
                row["通过"] = suggestion
            if "备注" in headers:
                row["备注"] = note

        rendered_table = [
            "| " + " | ".join(headers) + " |",
            "| " + " | ".join(["---"] * len(headers)) + " |",
        ]
        for row in rows:
            rendered_table.append("| " + " | ".join(str(row.get(header, "")) for header in headers) + " |")

        table_pattern = r'^\|.*\|\n\|[\-\|\s:]+\|\n(?:\|.*\|\n?)*'
        replacement = "\n".join(rendered_table) + "\n"
        new_content = re.sub(table_pattern, replacement, content, count=1, flags=re.MULTILINE)
        self.write(self.core_pool_path, new_content)

    def get_journal_path(self, date: str) -> str:
        """
        返回某天的日志路径

        Args:
            date: 日期字符串，格式 YYYY-MM-DD

        Returns:
            相对于 vault 根目录的日志路径
        """
        return os.path.join(self.journal_dir, f"{date}.md")

    def write_journal(self, date: str, content: str) -> None:
        """
        写日志

        Args:
            date: 日期字符串，格式 YYYY-MM-DD
            content: 日志内容
        """
        relative_path = self.get_journal_path(date)
        self.write(relative_path, content)

    def get_signal_snapshot_path(self, date: str) -> str:
        """返回某天的信号快照路径。"""
        return os.path.join(self.signal_snapshot_dir, f"{date}.md")

    def render_signal_snapshot(self, bundle: dict) -> str:
        """渲染信号快照 markdown。"""
        bundle = bundle or {}
        market = bundle.get("market_snapshot", {}) or {}
        pool = bundle.get("pool_snapshot", {}) or {}
        decision = bundle.get("today_decision", {}) or {}
        candidates = bundle.get("scored_candidates", []) or []
        status = bundle.get("status", "missing")
        snapshot_date = bundle.get("snapshot_date", "")
        missing = bundle.get("missing_components", [])

        lines = [
            "---",
            f"updated_at: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "type: signal_snapshot",
            f"date: {snapshot_date}",
            f"status: {status}",
            f"tags: [信号快照, 自动更新]",
            "---",
            "",
            f"# 信号快照 · {snapshot_date}",
            "",
            f"> 状态: **{status}**"
        ]

        if missing:
            lines.append(f"> 缺失组件: {', '.join(missing)}")

        # 大盘信号
        market_signal = market.get("signal", "") or decision.get("market_signal", "")
        signal_emoji = {"GREEN": "🟢", "YELLOW": "🟡", "RED": "🔴", "CLEAR": "⚪"}.get(market_signal, "⚪")
        signal_text = {"GREEN": "偏强", "YELLOW": "震荡", "RED": "转弱", "CLEAR": "观望"}.get(market_signal, market_signal)
        lines.extend([
            "",
            "## 大盘信号",
            "",
            f"| 指标 | 数值 |",
            "|------|------|",
            f"| 整体信号 | {signal_emoji} {signal_text} |",
        ])

        indices = market.get("indices") or {}
        if isinstance(indices, dict) and indices:
            lines.extend(["| 指数 | 最新 | 涨跌% | 信号 |", "|------|------|------|------|"])
            for name, info in list(indices.items())[:5]:
                if isinstance(info, dict):
                    close = info.get("close", info.get("price", 0))
                    chg = info.get("change_pct", info.get("chg_pct", 0))
                    sig = info.get("signal", "")
                    lines.append(f"| {name} | {close:.2f} | {chg:+.2f}% | {sig} |")
        else:
            lines.append("| 暂无指数数据 | — | — | — |")

        # 今日决策
        action = decision.get("action", decision.get("decision", ""))
        lines.extend([
            "",
            "## 今日决策",
            "",
            f"| 项目 | 数值 |",
            "|------|------|",
            f"| 决策动作 | {action or '—'} |",
            f"| 市场信号 | {market_signal or '—'} |",
        ])
        reasons = decision.get("reasons", []) or []
        if reasons:
            lines.append("| 原因 | " + "；".join(str(r) for r in reasons[:3]) + " |")
        risk_state = decision.get("risk", {}).get("state", "") if isinstance(decision.get("risk"), dict) else ""
        if risk_state:
            lines.append(f"| 风控状态 | {risk_state} |")

        # 池子摘要
        summary = pool.get("summary", {}) or {}
        lines.extend([
            "",
            "## 池子摘要",
            "",
            f"| 类别 | 数量 |",
            "|------|------|",
            f"| 核心池 | {int(summary.get('core_count', summary.get('core', 0) or 0))} |",
            f"| 观察池 | {int(summary.get('watch_count', summary.get('watch', 0) or 0))} |",
            f"| 其他 | {int(summary.get('other_count', summary.get('avoid', 0) or 0))} |",
        ])

        # 候选股 Top5
        top = sorted(candidates, key=lambda c: float(c.get("total_score", c.get("score", 0)) or 0) if isinstance(c.get("total_score"), (int, float)) else 0, reverse=True)[:5]
        if top:
            lines.extend([
                "",
                "## 候选股 Top5",
                "",
                "| 排名 | 代码 | 名称 | 总分 | 技术 | 基本 | 资金 | 舆情 | 否决 |",
                "|------|------|------|------|------|------|------|------|------|",
            ])
            for i, c in enumerate(top, 1):
                total = c.get("total_score", c.get("score", 0)) or 0
                tech = c.get("technical_score", 0) or 0
                fund = c.get("fundamental_score", 0) or 0
                flow = c.get("flow_score", 0) or 0
                sent = c.get("sentiment_score", 0) or 0
                veto = "⚠️" if c.get("veto_triggered") else "✅"
                name = c.get("name", c.get("code", ""))
                code = c.get("code", "")
                lines.append(f"| {i} | {code} | {name} | {float(total):.1f} | {float(tech):.1f} | {float(fund):.1f} | {float(flow):.1f} | {float(sent):.1f} | {veto} |")
        else:
            lines.extend([
                "",
                "## 候选股 Top5",
                "",
                "_暂无候选股数据_",
            ])

        lines.extend([
            "",
            "---",
            "",
            f"> 本页由 `load_daily_signal_snapshot_bundle()` 自动投影生成。",
            f"> history_group_id: `{bundle.get('history_group_id', '') or '<auto>'}`",
        ])
        return "\n".join(lines)

    def write_signal_snapshot(self, date_str: str) -> str:
        """
        写入信号快照。从 SQLite 加载当日信号束并渲染到 vault。

        Args:
            date_str: 日期字符串，格式 YYYY-MM-DD

        Returns:
            写入文件的绝对路径
        """
        from scripts.state.service import load_daily_signal_snapshot_bundle
        bundle = load_daily_signal_snapshot_bundle(date_str, allow_pool_fallback=True)
        relative_path = self.get_signal_snapshot_path(date_str)
        self.write(relative_path, self.render_signal_snapshot(bundle))
        return self._full_path(relative_path)

    def get_daily_output_path(self, date: str) -> str:
        """返回某天的当日输出索引路径。"""
        return os.path.join(self.daily_output_dir, f"{date}.md")

    def render_daily_output_index(self, date_str: str, runs_dir: str) -> str:
        """
        渲染当日运行输出索引 markdown。

        Args:
            date_str: 日期字符串，格式 YYYY-MM-DD
            runs_dir: data/runs 的绝对路径
        """
        import json
        from pathlib import Path

        runs_path = Path(runs_dir) / date_str
        if not runs_path.exists():
            return ""

        all_files = sorted(runs_path.glob("*.json"), key=lambda p: p.name)
        if not all_files:
            return ""

        # 按 pipeline 分组，每组取最新一个 run
        latest_by_pipeline: dict[str, dict] = {}
        for f in all_files:
            try:
                with open(f, encoding="utf-8") as fp:
                    try:
                        data = json.load(fp)
                    except Exception as e:
                        raise VaultError(f"Failed to load JSON from {f}: {e}") from e
            except VaultError:
                continue
            pipeline = data.get("pipeline", "run")
            started = data.get("started_at", "")
            if pipeline not in latest_by_pipeline or started > latest_by_pipeline[pipeline].get("started_at", ""):
                latest_by_pipeline[pipeline] = data

        lines = [
            "---",
            f"updated_at: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "type: daily_output_index",
            f"date: {date_str}",
            "tags: [当日输出, 自动更新]",
            "---",
            "",
            f"# 当日输出 · {date_str}",
            "",
            f"共 {len(all_files)} 个运行记录，{len(latest_by_pipeline)} 个 pipeline。",
            "",
            "## Pipeline 运行状态",
            "",
            "| Pipeline | 状态 | 运行时长 | 开始时间 | 关键指标 |",
            "|------|------|------|------|------|",
        ]

        # 映射 pipeline 中文名
        pipeline_names = {
            "morning": "盘前",
            "noon": "午间",
            "evening": "收盘",
            "scoring": "评分",
            "screener": "选股",
            "sentiment": "舆情",
            "hk_monitor": "港股监控",
            "hk": "港股监控",
            "weekly": "周报",
            "monthly": "月报",
        }

        for pipeline in sorted(latest_by_pipeline.keys()):
            data = latest_by_pipeline[pipeline]
            status = data.get("status", "unknown")
            status_icon = {"success": "✅", "warning": "⚠️", "error": "❌", "skipped": "⏭️", "blocked": "🚫"}.get(status, "❓")
            duration = data.get("duration_seconds", 0) or 0
            started = data.get("started_at", "")[:16] if data.get("started_at") else "—"
            details = data.get("details", {}) or {}

            # 提取关键 KPI
            kpi_parts = []
            result = data.get("result", {}) or {}
            if "market_signal" in result:
                kpi_parts.append(f"信号:{result['market_signal']}")
            if "candidate_count" in details:
                kpi_parts.append(f"候选:{details['candidate_count']}")
            if "actionable_count" in details:
                kpi_parts.append(f"可操作:{details['actionable_count']}")
            if "market_signal" in details:
                kpi_parts.append(f"信号:{details['market_signal']}")
            if "signal" in result:
                kpi_parts.append(f"信号:{result['signal']}")
            if "open_count" in details:
                kpi_parts.append(f"开仓:{details['open_count']}")
            if "filled_count" in details:
                kpi_parts.append(f"成交:{details['filled_count']}")
            if "pool_actions_count" in details:
                kpi_parts.append(f"池调整:{details['pool_actions_count']}")

            kpi = " | ".join(kpi_parts) if kpi_parts else "—"
            name_cn = pipeline_names.get(pipeline, pipeline)
            lines.append(f"| {name_cn} | {status_icon} {status} | {duration:.1f}s | {started} | {kpi} |")

        # 交叉链接
        journal_link = f"[[{date_str}]]"
        decision_link = "[[今日决策]]"
        account_link = "[[账户总览]]"
        signal_link = f"[[{date_str}]]"
        pool_link = "[[核心池]]"

        lines.extend([
            "",
            "## 相关文件",
            "",
            f"- 日志: {journal_link}",
            f"- 今日决策: {decision_link}",
            f"- 账户总览: {account_link}",
            f"- 信号快照: {signal_link}",
            f"- 核心池: {pool_link}",
            "",
            "---",
            "",
            f"> 本页由 `render_daily_output_index()` 自动投影生成。",
            f"> 数据来源: `data/runs/{date_str}/` ({len(all_files)} 个 JSON)",
        ])
        return "\n".join(lines)

    def write_daily_output_index(self, date_str: str, runs_dir: str) -> str:
        """
        写入当日输出索引。从 data/runs/ 聚合当日所有运行结果。

        Args:
            date_str: 日期字符串，格式 YYYY-MM-DD
            runs_dir: data/runs 的绝对路径

        Returns:
            写入文件的绝对路径
        """
        content = self.render_daily_output_index(date_str, runs_dir)
        if not content:
            return ""
        relative_path = self.get_daily_output_path(date_str)
        self.write(relative_path, content)
        return self._full_path(relative_path)

    def get_candidate_pool_path(self) -> str:
        """返回候选池总览路径（每日快照则传入日期）。"""
        return os.path.join(self.candidate_pool_dir, "候选池总览.md")

    def render_candidate_pool(self, pool_entries: list[dict], score_history: list[dict]) -> str:
        """
        渲染候选池总览 markdown。

        Args:
            pool_entries: 当前池条目列表（来自 load_pool_snapshot）
            score_history: 每日评分列表，每项含 snapshot_date + candidates
        """
        # 构建近 N 天分数映射
        TREND_DAYS = 5
        score_by_code: dict[str, list[tuple[str, float]]] = {}
        for day_data in score_history[-TREND_DAYS:]:
            date = day_data.get("snapshot_date", "")
            candidates = day_data.get("candidates", []) or []
            for c in candidates:
                code = str(c.get("code", "")).strip()
                if not code:
                    continue
                score = float(c.get("total_score", c.get("score", 0)) or 0)
                score_by_code.setdefault(code, []).append((date, score))

        # 按 bucket + score 排序
        BUY_THRESHOLD = 7.0
        WATCH_UPPER = 5.5
        WATCH_LOWER = 4.5

        edge_buy, edge_watch, core_stable, watch_stable, veto_list, avoid_list = [], [], [], [], [], []

        for entry in pool_entries:
            code = str(entry.get("code", "")).strip()
            name = str(entry.get("name", code)).strip()
            bucket = str(entry.get("bucket", "")).strip()
            score = float(entry.get("total_score", 0) or 0)
            veto_triggered = bool(entry.get("veto_triggered", False))
            veto_signals = entry.get("veto_signals", []) or []
            if isinstance(veto_signals, str):
                veto_signals = [veto_signals]
            metadata = entry.get("metadata", {}) or {}
            added_date = metadata.get("added_date", entry.get("added_date", ""))

            # 分数趋势
            history = score_by_code.get(code, [])
            if len(history) >= 2:
                latest = history[-1][1] if history else 0
                prev = history[-2][1] if len(history) > 1 else latest
                delta = latest - prev
                trend_icon = "📈" if delta > 0.3 else ("📉" if delta < -0.3 else "➡️")
                trend_text = f"{trend_icon} {delta:+.1f}（{len(history)}天）"
            elif len(history) == 1:
                trend_text = f"📍 首次出现在 {history[0][0]}"
            else:
                trend_text = "❓ 无历史"

            row = {
                "code": code,
                "name": name,
                "bucket": bucket,
                "score": score,
                "veto": veto_triggered,
                "veto_signals": veto_signals,
                "trend": trend_text,
                "added_date": added_date,
            }

            if veto_triggered:
                veto_list.append(row)
            elif bucket == "core":
                if WATCH_UPPER <= score < BUY_THRESHOLD:
                    edge_buy.append(row)
                else:
                    core_stable.append(row)
            elif bucket == "watch":
                if WATCH_LOWER <= score < WATCH_UPPER:
                    edge_watch.append(row)
                else:
                    watch_stable.append(row)
            else:
                avoid_list.append(row)

        def render_bucket_table(rows: list[dict], title: str) -> list[str]:
            if not rows:
                return []
            lines = ["", f"### {title}", ""]
            lines.extend([
                "| 代码 | 名称 | 评分 | 否决 | 趋势 | 入池日期 |",
                "|------|------|------|------|------|------|",
            ])
            for r in rows:
                if r["veto"]:
                    veto_labels = [VETO_LABEL_MAP.get(s, s) for s in r["veto_signals"]]
                    veto_flag = "⚠️ " + " ".join(veto_labels)
                else:
                    veto_flag = "✅"
                lines.append(f"| {r['code']} | {r['name']} | {r['score']:.1f} | {veto_flag} | {r['trend']} | {r['added_date'] or '—'} |")
            return lines

        lines = [
            "---",
            f"updated_at: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "type: candidate_pool_overview",
            "tags: [候选池, 自动更新]",
            "---",
            "",
            "# 候选池总览",
            "",
            f"> 统计日期: {datetime.now().strftime('%Y-%m-%d')} | 统计周期: 近 {TREND_DAYS} 天评分趋势",
        ]

        # 摘要
        all_stocks = pool_entries or []
        lines.extend([
            "",
            "## 池子摘要",
            "",
            f"| 类别 | 数量 |",
            "|------|------|",
            f"| 核心池·稳定 | {len(core_stable)} |",
            f"| 核心池·买入边缘 | {len(edge_buy)} |",
            f"| 观察池·稳定 | {len(watch_stable)} |",
            f"| 观察池·边缘 | {len(edge_watch)} |",
            f"| 否决池 | {len(veto_list)} |",
            f"| 规避 | {len(avoid_list)} |",
            "",
            "> 买入边缘：核心池评分 5.5–6.9，距买入阈值 < 2 分",
            "> 观察边缘：观察池评分 4.5–5.4，接近但未达买入线",
        ])

        for section_rows, title in [
            (core_stable, "核心池·稳定（评分 ≥ 7.0 或 稳定）"),
            (edge_buy, "核心池·买入边缘（评分 5.5–6.9）"),
            (watch_stable, "观察池·稳定（评分 ≥ 5.5）"),
            (edge_watch, "观察池·边缘（评分 4.5–5.4）"),
            (veto_list, "否决池（veto_triggered = True）"),
            (avoid_list, "规避池"),
        ]:
            lines.extend(render_bucket_table(section_rows, title))

        lines.extend([
            "",
            "---",
            "",
            f"> 本页由 `render_candidate_pool()` 自动投影生成。",
            f"> 数据来源: 筛选结果_综合_*.md（近 {TREND_DAYS} 天趋势）",
        ])
        return "\n".join(lines)

    def write_candidate_pool(self, snapshot_date: str) -> str:
        """
        写入候选池总览。直接从评分报告 md 文件读取近 5 天评分历史。

        Args:
            snapshot_date: 日期字符串，格式 YYYY-MM-DD

        Returns:
            写入文件的绝对路径
        """
        import re
        from datetime import timedelta
        from scripts.state.service import load_pool_snapshot_history

        # 当前池条目（从 SQLite，pool entries 通常可靠）
        pool_bundle = load_pool_snapshot_history(snapshot_date=snapshot_date, limit=1)
        pool_latest = pool_bundle.get("latest", {}) or {}
        entries = pool_latest.get("entries", []) or []

        # 近 5 天评分历史（从 md 文件，而非 SQLite candidate_snapshot_history）
        TREND_DAYS = 5
        score_history = []
        for days_ago in range(TREND_DAYS):
            date_offset = datetime.now() - timedelta(days=days_ago)
            date_str = date_offset.strftime("%Y-%m-%d")
            date_glob = date_offset.strftime("%Y%m%d")
            # 找当天最近的综合报告
            pattern = f"筛选结果_综合_{date_glob}_*.md"
            report_dir = os.path.join(self.vault_path, self.screening_results_dir)
            candidates = []
            try:
                matches = sorted(Path(report_dir).glob(pattern))
                if matches:
                    content = matches[-1].read_text(encoding="utf-8")
                    # 解析 markdown 表格：| # | 股票 | 代码 | 四维总分 | ... |
                    for line in content.splitlines():
                        m = re.match(r"\|\s*\d+\s*\|\s*([^|]+?)\s*\|\s*(\d{6})\s*\|\s*\*\*?([\d.]+)\*?\*\*?\s*\|", line.strip())
                        if m:
                            name, code, score = m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
                            try:
                                candidates.append({"code": code, "name": name, "total_score": float(score)})
                            except ValueError:
                                pass
            except Exception:
                pass
            if candidates:
                score_history.append({"snapshot_date": date_str, "candidates": candidates})

        content = self.render_candidate_pool(entries, score_history)
        relative_path = self.get_candidate_pool_path()
        self.write(relative_path, content)
        return self._full_path(relative_path)

    def get_stock_explanation_path(self, code: str) -> str:
        """返回某只股票的个股解释路径。"""
        return os.path.join(self.stock_explain_dir, f"{code}.md")

    def _parse_technical_detail(self, detail: str) -> dict:
        """解析技术面 detail 字符串，返回结构化 dict。"""
        result = {"金叉": False, "量比": 0.0, "RSI": 0.0, "排列": 0.0, "动量": 0.0}
        if not detail:
            return result
        import re
        gc = re.search(r"金叉:([\d.]+)/1(.)", detail)
        if gc:
            result["金叉"] = gc.group(2) == "✓"
        vr = re.search(r"量比:([\d.]+)/0\.5\(([\d.]+)\)", detail)
        if vr:
            result["量比"] = float(vr.group(2))
        rsi = re.search(r"RSI:[\d.]+/0\.5\((\d+)\)", detail)
        if rsi:
            result["RSI"] = float(rsi.group(1))
        arr = re.search(r"排列:([\d.]+)/0\.5", detail)
        if arr:
            result["排列"] = float(arr.group(1))
        mom = re.search(r"动量:([\d.]+)/0\.5", detail)
        if mom:
            result["动量"] = float(mom.group(1))
        return result

    def _interpret_technical(self, parsed: dict) -> list[str]:
        lines = []
        gc = parsed.get("金叉", False)
        vr = parsed.get("量比", 0.0)
        rsi = parsed.get("RSI", 0.0)
        arr = parsed.get("排列", 0.0)
        mom = parsed.get("动量", 0.0)
        if gc:
            lines.append("- 🟢 已出现 5/20 日均线金叉（短期趋势向上）")
        else:
            lines.append("- ⚪ 未出现金叉（需等待均线交叉信号）")
        if vr >= 1.5:
            lines.append(f"- 🟢 量比 {vr:.1f}x，成交量活跃（>1.5x 为放量）")
        elif vr >= 1.0:
            lines.append(f"- 🟡 量比 {vr:.1f}x，成交量正常")
        else:
            lines.append(f"- 🔴 量比 {vr:.1f}x，成交量偏少")
        if rsi > 70:
            lines.append(f"- 🔴 RSI {rsi:.0f}，处于超买区域（>70）")
        elif rsi < 30:
            lines.append(f"- 🟢 RSI {rsi:.0f}，处于超卖区域，有反弹可能")
        else:
            lines.append(f"- 🟡 RSI {rsi:.0f}，处于正常区间（30-70）")
        if arr >= 0.5:
            lines.append("- 🟢 均线多头排列（短期 > 中期 > 长期）")
        if mom >= 0.3:
            lines.append("- 🟢 近期有正向动量")
        return lines

    def _interpret_fundamental(self, detail: str) -> list[str]:
        """解析基本面 detail，返回中文解读。"""
        lines = []
        if not detail or "数据错误" in str(detail):
            lines.append("- ❌ 基本面数据获取失败")
            return lines
        import re
        roe_m = re.search(r"ROE:([\d.]+)/1", detail)
        rev_m = re.search(r"营收:([\d.]+)/1", detail)
        cf_m = re.search(r"现金流:([\d.]+)/1", detail)
        miss_m = re.search(r"缺失:(\S+)", detail)
        if roe_m:
            roe = float(roe_m.group(1))
            if roe >= 1.0:
                lines.append(f"- 🟢 ROE {roe:.0%}，盈利能力优秀（≥15%）")
            elif roe >= 0.7:
                lines.append(f"- 🟡 ROE {roe:.0%}，盈利能力良好（10-15%）")
            elif roe >= 0.4:
                lines.append(f"- 🟡 ROE {roe:.0%}，盈利能力一般（5-10%）")
            else:
                lines.append(f"- 🔴 ROE {roe:.0%}，盈利能力偏弱（<5%）")
        if rev_m:
            rev = float(rev_m.group(1))
            if rev >= 1.0:
                lines.append(f"- 🟢 营收增长 {rev:.0%}，增速强劲（≥20%）")
            elif rev >= 0.7:
                lines.append(f"- 🟡 营收增长 {rev:.0%}，增速稳健")
            elif rev > 0:
                lines.append(f"- 🟡 营收增长 {rev:.0%}，增速较低")
            else:
                lines.append(f"- 🔴 营收增长 {rev:.0%}，营收下滑")
        if cf_m:
            cf = float(cf_m.group(1))
            if cf >= 0.5:
                lines.append("- 🟢 经营现金流为正，财务健康")
            else:
                lines.append("- 🔴 经营现金流为负或数据缺失")
        if miss_m:
            lines.append(f"- ⚠️ 数据缺失: {miss_m.group(1)}")
        return lines

    def _interpret_veto(self, veto_signals: list) -> list[str]:
        """将否决信号列表转为中文说明。"""
        VETO_MAP = {
            "below_ma20": "股价位于 20 日均线下方，当前趋势偏弱",
            "limit_up_today": "今日涨停，追高风险极大",
            "consecutive_outflow": "连续资金净流出，上涨动力不足",
            "consecutive_outflow_warn": "资金连续流出，疑似洗盘（警告）",
            "red_market": "大盘整体下跌，逆势操作风险高",
            "ma20_trend_down": "20 日均线趋势向下，中期趋势偏弱",
            "earnings_bomb": "近期业绩暴雷（净利润同比大幅下降）",
            "low_liquidity": "流动性不足（大单成交稀疏）",
            "score_error": "评分计算异常",
        }
        lines = []
        for sig in veto_signals:
            msg = VETO_MAP.get(str(sig), f"否决信号: {sig}")
            if sig == "consecutive_outflow_warn":
                lines.append(f"- ⚠️ {msg}")
            else:
                lines.append(f"- 🔴 {msg}")
        return lines

    def render_stock_explanation(self, candidate: dict) -> str:
        """
        渲染单只股票的评分解释 markdown。

        Args:
            candidate: 评分候选股对象（来自 batch_score 输出）
        """
        code = str(candidate.get("code", "")).strip()
        name = str(candidate.get("name", code)).strip()
        total = float(candidate.get("total_score", 0) or 0)
        tech = float(candidate.get("technical_score", 0) or 0)
        fund = float(candidate.get("fundamental_score", 0) or 0)
        flow = float(candidate.get("flow_score", 0) or 0)
        sent = float(candidate.get("sentiment_score", 0) or 0)
        veto_triggered = bool(candidate.get("veto_triggered", False))
        veto_signals = candidate.get("veto_signals", []) or []
        if isinstance(veto_signals, str):
            veto_signals = [veto_signals]
        bucket = candidate.get("bucket", "")
        recommendation = candidate.get("recommendation", "")

        # 推荐信号
        REC_MAP = {"buy": "🟢 建议买入", "watch": "🟡 观察", "avoid": "🔴 建议规避", "manual_review": "⚠️ 人工复查"}
        rec_text = REC_MAP.get(str(recommendation), recommendation or "—")

        # 评分条
        MAX_SCORE = 10.0
        bar_len = 20
        def score_bar(score: float, max_s: float = MAX_SCORE) -> str:
            filled = int(round(score / max_s * bar_len))
            return "█" * filled + "░" * (bar_len - filled)

        lines = [
            "---",
            f"updated_at: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "type: stock_explanation",
            f"code: {code}",
            f"bucket: {bucket}",
            f"total_score: {total}",
            "tags: [个股解释, 自动更新]",
            "---",
            "",
            f"# {name}（{code}）评分解释",
            "",
            f"> 推荐信号: **{rec_text}** | 池分类: **{bucket or '—'}**",
            "",
            "## 综合评分",
            "",
            f"| 总分 | {score_bar(total)} | {total:.1f} / {MAX_SCORE} |",
            "",
            f"| 维度 | 评分 | 状态 | 满分 |",
            "|------|------|------|------|",
            f"| 技术面 | {tech:.1f} | {self._score_emoji(tech, 3.0)} | 3.0 |",
            f"| 基本面 | {fund:.1f} | {self._score_emoji(fund, 3.0)} | 3.0 |",
            f"| 资金面 | {flow:.1f} | {self._score_emoji(flow, 2.0)} | 2.0 |",
            f"| 舆情面 | {sent:.1f} | {self._score_emoji(sent, 2.0)} | 2.0 |",
        ]

        # 技术面解读
        tech_detail = candidate.get("technical_detail", "") or ""
        if "数据错误" in tech_detail:
            lines.extend(["", "## 技术面解读", "", "❌ 技术面数据获取失败，无法解读。"])
        else:
            parsed = self._parse_technical_detail(tech_detail)
            tech_lines = self._interpret_technical(parsed)
            lines.extend(["", "## 技术面解读", ""])
            lines.extend(tech_lines if tech_lines else ["- 无明显技术信号"])

        # 基本面解读
        fund_detail = candidate.get("fundamental_detail", "") or ""
        lines.extend(["", "## 基本面解读", ""])
        fund_lines = self._interpret_fundamental(fund_detail)
        lines.extend(fund_lines if fund_lines else ["- 无基本面数据"])

        # 资金面
        flow_detail = candidate.get("flow_detail", "") or ""
        if flow_detail and "数据错误" not in flow_detail:
            lines.extend(["", "## 资金面解读", ""])
            lines.append(f"- 资金评分: {flow:.1f}/2.0")
            if "净流入" in flow_detail or "inflow" in flow_detail.lower():
                lines.append("- 🟢 主力资金呈净流入")
            elif "净流出" in flow_detail or "outflow" in flow_detail.lower():
                lines.append("- 🔴 主力资金呈净流出")
            else:
                lines.append(f"- 🟡 资金流向详情: {flow_detail[:100]}")
        else:
            lines.extend(["", "## 资金面解读", "", "- 无资金流向数据"])

        # 舆情
        sent_detail = candidate.get("sentiment_detail", "") or ""
        if sent_detail and "数据错误" not in sent_detail:
            lines.extend(["", "## 舆情解读", ""])
            if sent >= 1.5:
                lines.append(f"- 🟢 舆情正面，评分 {sent:.1f}/2.0")
            elif sent >= 1.0:
                lines.append(f"- 🟡 舆情中性，评分 {sent:.1f}/2.0")
            else:
                lines.append(f"- 🔴 舆情偏负面，评分 {sent:.1f}/2.0")
        else:
            lines.extend(["", "## 舆情解读", "", "- 无舆情数据"])

        # 否决信号
        if veto_triggered and veto_signals:
            veto_lines = self._interpret_veto(veto_signals)
            lines.extend(["", "## 否决信号（触发一票否决）", ""])
            lines.extend(veto_lines)
        elif veto_signals:
            veto_lines = self._interpret_veto(veto_signals)
            lines.extend(["", "## 警告信号（仅预警，不否决）", ""])
            lines.extend(veto_lines)

        # 原始 detail
        if any([tech_detail, fund_detail, flow_detail, sent_detail]):
            lines.extend([
                "",
                "## 原始评分明细",
                "",
                "```",
            ])
            if tech_detail:
                lines.append(f"[技术] {tech_detail}")
            if fund_detail:
                lines.append(f"[基本] {fund_detail}")
            if flow_detail:
                lines.append(f"[资金] {flow_detail}")
            if sent_detail:
                lines.append(f"[舆情] {sent_detail}")
            lines.extend(["```", ""])

        lines.extend([
            "---",
            "",
            f"> 本页由 `render_stock_explanation()` 自动投影生成。",
            f"> 更新时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        ])
        return "\n".join(lines)

    def _score_emoji(self, score: float, max_score: float) -> str:
        ratio = score / max_score if max_score > 0 else 0
        if ratio >= 0.8:
            return "🟢 强"
        elif ratio >= 0.5:
            return "🟡 中"
        elif ratio > 0:
            return "🟠 弱"
        else:
            return "⚫ 无"

    def write_stock_explanations(self, candidates: list[dict], scope: str = "core") -> list[str]:
        """
        批量写入个股解释。对所有有 detail 的候选股生成解释文件。

        Args:
            candidates: 评分结果列表
            scope: 池分类，用于过滤（如 "core", "watch", "all"）

        Returns:
            写入文件的绝对路径列表
        """
        if scope != "all":
            candidates = [c for c in candidates if str(c.get("bucket", "")).strip() == scope]
        paths = []
        for c in candidates:
            code = str(c.get("code", "")).strip()
            if not code:
                continue
            # 只对有 detail 的股票生成解释
            has_detail = any([
                c.get("technical_detail"), c.get("fundamental_detail"),
                c.get("flow_detail"), c.get("sentiment_detail"),
            ])
            has_score = float(c.get("total_score", 0) or 0) > 0
            if not (has_score or has_detail):
                continue
            content = self.render_stock_explanation(c)
            relative_path = self.get_stock_explanation_path(code)
            self.write(relative_path, content)
            paths.append(self._full_path(relative_path))
        return paths


if __name__ == "__main__":
    # 简单测试
    v = ObsidianVault()
    print(f"Vault path: {v.vault_path}")
    print(f"Portfolio path: {v.portfolio_path}")
    print(f"Core pool path: {v.core_pool_path}")
    print(f"Journal path for 2026-04-08: {v.get_journal_path('2026-04-08')}")
