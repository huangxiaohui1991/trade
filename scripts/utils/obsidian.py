#!/usr/bin/env python3
"""
Obsidian Vault 文件读写工具
提供持仓、核心池、日志等文件的读写接口
"""

import os
import shutil
import re
from datetime import datetime
from typing import Optional

# 导入 parser 模块的解析函数
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from scripts.utils.parser import parse_frontmatter, parse_md_table, parse_portfolio as parse_portfolio_file


class ObsidianVault:
    """Obsidian vault 文件读写工具"""

    def __init__(self, vault_path: Optional[str] = None):
        """
        初始化 Obsidian vault

        Args:
            vault_path: vault 根目录，默认为 ~/Documents/a-stock-trading
        """
        if vault_path is None:
            vault_path = os.path.expanduser("~/Documents/a-stock-trading")
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

        # 建立代码到新评分的映射
        score_map = {str(s.get('代码', '')): s for s in scores}

        # 更新表格中的评分
        for table in tables:
            for row in table.get('rows', []):
                code = str(row.get('代码', ''))
                if code in score_map:
                    new_score = score_map[code]
                    for key, value in new_score.items():
                        if key != '代码':
                            row[key] = value

        # 重新构建表格内容
        lines = content.split('\n')
        result_lines = []
        in_table = False
        table_idx = 0

        for line in lines:
            # 检测表格开始
            if '|' in line and not line.startswith('|---') and not result_lines:
                # 可能是在表头行
                if table_idx < len(tables) and tables[table_idx].get('headers'):
                    headers = tables[table_idx]['headers']
                    result_lines.append(line)
                    in_table = True
                    continue

            # 检测分隔线
            if re.match(r'\s*\|[\s\-:|]+\|', line):
                result_lines.append(line)
                continue

            # 表格数据行
            if in_table and '|' in line:
                cells = [c.strip() for c in line.split('|') if c.strip()]
                if table_idx < len(tables):
                    rows = tables[table_idx].get('rows', [])
                    if rows:
                        row = rows.pop(0)
                        new_cells = []
                        for h in headers:
                            new_cells.append(str(row.get(h, '')))
                        result_lines.append("| " + " | ".join(new_cells) + " |")
                        continue
                result_lines.append(line)
                continue

            # 表格结束检测
            if in_table and not line.strip().startswith('|'):
                in_table = False
                table_idx += 1

            result_lines.append(line)

        self.write(self.core_pool_path, '\n'.join(result_lines))

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
