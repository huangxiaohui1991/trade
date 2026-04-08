#!/usr/bin/env python3
"""
A股交易系统 - MD 文件解析器
解析 Obsidian MD 文件中的 YAML Front Matter 和表格数据

用法:
  python parser.py frontmatter <file_path>     解析 YAML Front Matter
  python parser.py table <file_path> [header]   解析 MD 表格
  python parser.py portfolio <file_path>         解析持仓汇总
  python parser.py journal <dir_path> [days]     解析最近N天日志
"""

import sys
import os
import json
import re
from datetime import datetime, timedelta
from typing import Optional


def parse_frontmatter(content: str) -> dict:
    """
    解析 YAML Front Matter
    支持格式:
    ---
    key: value
    tags: [tag1, tag2]
    ---
    """
    match = re.match(r'^---\s*\n(.*?)\n---', content, re.DOTALL)
    if not match:
        return {}

    yaml_text = match.group(1)
    result = {}

    for line in yaml_text.strip().split('\n'):
        line = line.strip()
        if not line or line.startswith('#'):
            continue

        if ':' in line:
            key, value = line.split(':', 1)
            key = key.strip()
            value = value.strip()

            # 解析数组 [a, b, c]
            if value.startswith('[') and value.endswith(']'):
                items = value[1:-1].split(',')
                result[key] = [item.strip().strip('"').strip("'") for item in items if item.strip()]
            # 解析数字
            elif re.match(r'^-?\d+(\.\d+)?$', value):
                result[key] = float(value) if '.' in value else int(value)
            # 解析布尔
            elif value.lower() in ('true', 'false'):
                result[key] = value.lower() == 'true'
            # 字符串
            else:
                result[key] = value.strip('"').strip("'")

    return result


def parse_md_table(content: str, header_match: Optional[str] = None) -> list:
    """
    解析 MD 表格为字典列表

    参数:
        content: MD 文件内容
        header_match: 可选，只解析包含此标题的表格
    """
    lines = content.split('\n')
    tables = []
    current_table = None
    headers = None

    for i, line in enumerate(lines):
        line = line.strip()

        # 检测表格头
        if '|' in line and not line.startswith('|---'):
            cells = [c.strip() for c in line.split('|') if c.strip()]

            # 下一行是分隔线则确认为表头
            if i + 1 < len(lines) and re.match(r'\s*\|[\s\-:|]+\|', lines[i + 1]):
                if header_match and header_match not in line:
                    headers = None
                    continue
                headers = cells
                current_table = []
                continue

        # 跳过分隔线
        if re.match(r'\s*\|[\s\-:|]+\|', line):
            continue

        # 解析数据行
        if headers and '|' in line:
            cells = [c.strip() for c in line.split('|') if c.strip()]
            if len(cells) >= len(headers):
                row = {}
                for j, h in enumerate(headers):
                    row[h] = cells[j] if j < len(cells) else ""
                current_table.append(row)
            elif not line.strip('| -'):
                # 表格结束
                if current_table:
                    tables.append({"headers": headers, "rows": current_table})
                headers = None
                current_table = None
        elif headers and not line:
            # 空行，表格结束
            if current_table:
                tables.append({"headers": headers, "rows": current_table})
            headers = None
            current_table = None

    # 最后一个表格
    if headers and current_table:
        tables.append({"headers": headers, "rows": current_table})

    return tables


def parse_portfolio(file_path: str) -> dict:
    """解析持仓汇总文件"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    frontmatter = parse_frontmatter(content)
    tables = parse_md_table(content)

    holdings = []
    for table in tables:
        for row in table["rows"]:
            holding = {}
            for key, value in row.items():
                # 尝试转换数字
                try:
                    if '.' in value:
                        holding[key] = float(value)
                    elif value.isdigit():
                        holding[key] = int(value)
                    else:
                        holding[key] = value
                except (ValueError, AttributeError):
                    holding[key] = value
            holdings.append(holding)

    return {
        "meta": frontmatter,
        "holdings": holdings,
        "count": len(holdings)
    }


def parse_journal_dir(dir_path: str, days: int = 7) -> dict:
    """解析最近N天的交易日志"""
    if not os.path.exists(dir_path):
        return {"journals": [], "count": 0}

    cutoff = datetime.now() - timedelta(days=days)
    journals = []

    for filename in sorted(os.listdir(dir_path), reverse=True):
        if not filename.endswith('.md'):
            continue

        # 从文件名提取日期 (格式: 2026-04-02.md)
        date_match = re.match(r'(\d{4}-\d{2}-\d{2})', filename)
        if not date_match:
            continue

        file_date = datetime.strptime(date_match.group(1), '%Y-%m-%d')
        if file_date < cutoff:
            continue

        file_path = os.path.join(dir_path, filename)
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        fm = parse_frontmatter(content)
        fm["_file"] = filename
        fm["_date"] = date_match.group(1)
        journals.append(fm)

    # 统计
    def safe_float(v, default=0):
        try:
            return float(v) if v else default
        except (ValueError, TypeError):
            return default

    total_pnl = sum(safe_float(j.get("daily_pnl", 0)) for j in journals)
    win_count = sum(1 for j in journals if safe_float(j.get("daily_pnl", 0)) > 0)
    loss_count = sum(1 for j in journals if safe_float(j.get("daily_pnl", 0)) < 0)
    trade_count = sum(int(safe_float(j.get("trades", 0))) for j in journals)

    return {
        "journals": journals,
        "count": len(journals),
        "total_pnl": total_pnl,
        "win_days": win_count,
        "loss_days": loss_count,
        "total_trades": trade_count,
        "win_rate": f"{win_count / len(journals):.1%}" if journals else "N/A"
    }


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1]
    path = sys.argv[2]

    if command == "frontmatter":
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()
        result = parse_frontmatter(content)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    elif command == "table":
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()
        header = sys.argv[3] if len(sys.argv) > 3 else None
        result = parse_md_table(content, header)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    elif command == "portfolio":
        result = parse_portfolio(path)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    elif command == "journal":
        days = int(sys.argv[3]) if len(sys.argv) > 3 else 7
        result = parse_journal_dir(path, days)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    else:
        print(f"未知命令: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
