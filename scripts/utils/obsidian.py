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
from typing import Optional

# 导入 parser 模块的解析函数
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from scripts.utils.parser import parse_frontmatter, parse_md_table, parse_portfolio as parse_portfolio_file
from scripts.engine.scorer import split_veto_signals


class ObsidianVault:
    """Obsidian vault 文件读写工具"""

    def __init__(self, vault_path: Optional[str] = None):
        """
        初始化 Obsidian vault

        Args:
            vault_path: vault 根目录，默认按以下优先级解析：
                1. 显式传入 vault_path
                2. 环境变量 AStockVault
                3. 当前仓库根目录
        """
        if vault_path is None:
            project_root = Path(__file__).resolve().parent.parent.parent
            vault_path = os.environ.get("AStockVault") or str(project_root)
        self.vault_path = os.path.abspath(vault_path)

        # 文件路径映射（Obsidian vault 根目录直接存放）
        self.portfolio_path = "01-持仓/portfolio.md"
        self.core_pool_path = "04-选股/核心池.md"
        self.watch_pool_path = "04-选股/观察池.md"
        self.journal_dir = "02-日志"

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

    def write(self, relative_path: str, content: str) -> None:
        """
        写文件（自动备份原文件）

        Args:
            relative_path: 相对于 vault 根目录的路径
            content: 文件内容
        """
        full_path = self._full_path(relative_path)

        # 确保目录存在
        os.makedirs(os.path.dirname(full_path), exist_ok=True)

        # 备份原文件
        self._backup(full_path)

        # 写入新内容
        with open(full_path, 'w', encoding='utf-8') as f:
            f.write(content)

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

    def read_core_pool(self) -> list:
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

    def _render_pool_table(self, entries: list, bucket: str) -> str:
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
                note = f"预警:{','.join(warnings)}"
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
                        reason = f"veto:{','.join(hard_veto)}"
                    elif warnings:
                        reason = f"预警:{','.join(warnings)}"
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

        def _safe_float(value, default=0.0) -> float:
            try:
                if isinstance(value, str):
                    value = value.replace("**", "").replace(",", "").strip()
                return float(value) if value not in [None, ""] else default
            except (TypeError, ValueError):
                return default

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
                note = "veto:" + ",".join(hard_veto)
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
                note = f"预警:{','.join(warning_signals)}"

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


if __name__ == "__main__":
    # 简单测试
    v = ObsidianVault()
    print(f"Vault path: {v.vault_path}")
    print(f"Portfolio path: {v.portfolio_path}")
    print(f"Core pool path: {v.core_pool_path}")
    print(f"Journal path for 2026-04-08: {v.get_journal_path('2026-04-08')}")
