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


def parse_user_reply(text: str) -> dict:
    """解析用户 Discord 回复

    支持格式：
      "止损挂了 {股票名} ¥{价格}"
      "止损触发了 {股票名} 成交¥{价格}"
      "取消止损 {股票名}"
      "止盈挂了 {股票名} ¥{价格}"
      "止盈触发了 {股票名} 成交¥{价格}"
      "取消止盈 {股票名}"

    返回: {
        "action": "挂单"|"触发"|"取消",
        "type": "止损"|"止盈",
        "stock": "股票名",
        "price": float (挂单价),
        "filled_price": float (成交价，触发时),
        "raw": str (原始文本)
    }
    """
    result = {
        "action": None,
        "type": None,
        "stock": None,
        "price": None,
        "filled_price": None,
        "raw": text
    }

    # 匹配价格：¥ 后面跟数字，支持逗号分隔
    price_pattern = r'¥([\d,]+(?:\.\d+)?)'

    # 止损/止盈挂了 格式：{type}挂了 {stock} ¥{price}
    m = re.match(r'^(止损|止盈)挂了\s+(.+?)\s+¥([\d,]+(?:\.\d+)?)$', text)
    if m:
        result["type"] = m.group(1)
        result["action"] = "挂单"
        result["stock"] = m.group(2)
        result["price"] = float(m.group(3).replace(',', ''))
        return result

    # 止损/止盈触发了 格式：{type}触发了 {stock} 成交¥{price}
    m = re.match(r'^(止损|止盈)触发了\s+(.+?)\s+成交¥([\d,]+(?:\.\d+)?)$', text)
    if m:
        result["type"] = m.group(1)
        result["action"] = "触发"
        result["stock"] = m.group(2)
        result["filled_price"] = float(m.group(3).replace(',', ''))
        return result

    # 取消止损/取消止盈 格式：取消{type} {stock}
    m = re.match(r'^取消(止损|止盈)\s+(.+)$', text)
    if m:
        result["type"] = m.group(1)
        result["action"] = "取消"
        result["stock"] = m.group(2)
        return result

    return result


def _test_parse_user_reply():
    """测试 parse_user_reply() 的六种格式"""
    test_cases = [
        # 格式1: 止损挂了
        ("止损挂了 杰瑞股份 ¥103.5", {
            "action": "挂单", "type": "止损", "stock": "杰瑞股份",
            "price": 103.5, "filled_price": None
        }),
        # 格式2: 止损触发了
        ("止损触发了 杰瑞股份 成交¥103.2", {
            "action": "触发", "type": "止损", "stock": "杰瑞股份",
            "price": None, "filled_price": 103.2
        }),
        # 格式3: 取消止损
        ("取消止损 杰瑞股份", {
            "action": "取消", "type": "止损", "stock": "杰瑞股份",
            "price": None, "filled_price": None
        }),
        # 格式4: 止盈挂了
        ("止盈挂了 杰瑞股份 ¥10,500", {
            "action": "挂单", "type": "止盈", "stock": "杰瑞股份",
            "price": 10500.0, "filled_price": None
        }),
        # 格式5: 止盈触发了
        ("止盈触发了 杰瑞股份 成交¥10,500.5", {
            "action": "触发", "type": "止盈", "stock": "杰瑞股份",
            "price": None, "filled_price": 10500.5
        }),
        # 格式6: 取消止盈
        ("取消止盈 杰瑞股份", {
            "action": "取消", "type": "止盈", "stock": "杰瑞股份",
            "price": None, "filled_price": None
        }),
    ]

    print("Testing parse_user_reply()...")
    all_passed = True
    for text, expected in test_cases:
        result = parse_user_reply(text)
        passed = (
            result["action"] == expected["action"] and
            result["type"] == expected["type"] and
            result["stock"] == expected["stock"] and
            result["price"] == expected["price"] and
            result["filled_price"] == expected["filled_price"]
        )
        status = "PASS" if passed else "FAIL"
        if not passed:
            all_passed = False
            print(f"  [{status}] {text}")
            print(f"    Expected: {expected}")
            print(f"    Got:      {result}")
        else:
            print(f"  [{status}] {text}")

    print()
    if all_passed:
        print("All 6 tests PASSED!")
    else:
        print("Some tests FAILED!")
    return all_passed


if __name__ == "__main__":
    main()
