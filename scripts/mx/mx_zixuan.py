#!/usr/bin/env python3
"""
mx_zixuan — 妙想自选股管理

支持查询、添加、删除东方财富自选股。

用法：
  python -m scripts.mx.mx_zixuan query
  python -m scripts.mx.mx_zixuan add "贵州茅台"
  python -m scripts.mx.mx_zixuan delete "贵州茅台"
  python -m scripts.mx.mx_zixuan "把比亚迪加入自选"
"""

import os
import sys
import csv
import json
import argparse
from pathlib import Path
from typing import Dict, List, Any

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts.mx.client import MXBaseClient, _PROJECT_ROOT as ROOT
from scripts.utils.logger import get_logger

_logger = get_logger("mx.zixuan")
OUTPUT_DIR = ROOT / "data" / "mx_output"


def safe_filename(s: str, max_len: int = 80) -> str:
    s = s.replace(" ", "_").replace("/", "_").replace("\\", "_").replace(":", "_")
    s = s.replace("*", "_").replace("?", "_").replace('"', "_").replace("<", "_").replace(">", "_")
    s = s.replace("|", "_")[:max_len]
    return s or "query"


class MXZixuan(MXBaseClient):
    """妙想自选股管理客户端"""

    def query(self) -> Dict[str, Any]:
        """查询自选股列表"""
        return self._post("/api/claw/self-select/get", {})

    def manage(self, query: str) -> Dict[str, Any]:
        """添加或删除自选股"""
        return self._post("/api/claw/self-select/manage", {"query": query})


def format_query_result(result: Dict, output_dir: Path):
    if result.get("status") != 0 and result.get("code") != 0:
        print(f"❌ 查询失败: {result.get('message', '未知错误')}", file=sys.stderr)
        return

    data = result.get("data", {})
    all_results = data.get("allResults", {})
    result_data = all_results.get("result", {})
    columns = result_data.get("columns", [])
    data_list = result_data.get("dataList", [])

    if not data_list:
        print("ℹ️  自选股列表为空")
        return

    display_fields = [
        ("SECURITY_CODE", "股票代码", 8),
        ("SECURITY_SHORT_NAME", "股票名称", 8),
        ("NEWEST_PRICE", "最新价(元)", 10),
        ("CHG", "涨跌幅(%)", 10),
        ("PCHG", "涨跌额(元)", 10),
    ]

    print("📊 我的自选股列表")
    print("=" * 80)
    header = " | ".join([f"{name:<{width}}" for _, name, width in display_fields])
    print(header)
    print("-" * 80)

    for stock in data_list:
        row = []
        for key, _, width in display_fields:
            value = stock.get(key, "-")
            if key == "CHG" and value != "-":
                try:
                    chg = float(value)
                    value = f"+{value}%" if chg > 0 else f"{value}%"
                except (ValueError, TypeError):
                    pass
            row.append(f"{str(value):<{width}}")
        print(" | ".join(row))

    print("-" * 80)
    print(f"共 {len(data_list)} 只自选股")

    # 保存 CSV
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "mx_zixuan_我的自选股列表.csv"
    column_name_map = {}
    fieldnames = []
    for col in columns:
        title = col.get("title", col.get("key", "unknown"))
        key = col.get("key", "unknown")
        column_name_map[key] = title
        fieldnames.append(title)

    csv_rows = []
    for stock in data_list:
        csv_row = {title: stock.get(key, "") for key, title in column_name_map.items()}
        csv_rows.append(csv_row)

    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in csv_rows:
            writer.writerow(row)

    json_path = output_dir / "mx_zixuan_我的自选股列表_raw.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n✅ CSV: {csv_path}")
    print(f"📄 原始JSON: {json_path}")


def main():
    parser = argparse.ArgumentParser(description="妙想自选管理")
    parser.add_argument("command", nargs="?", help="命令: query/add/delete 或自然语言")
    parser.add_argument("stock", nargs="?", help="股票名称或代码")
    parser.add_argument("--output-dir", dest="output_dir", help=f"输出目录，默认 {OUTPUT_DIR}")
    args = parser.parse_args()

    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR

    mx = MXZixuan()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    command = args.command.lower()

    if command in ["query", "list", "查询", "列表"]:
        result = mx.query()
        format_query_result(result, output_dir)
    elif command in ["add", "添加", "增加"] and args.stock:
        result = mx.manage(f"把{args.stock}添加到我的自选股列表")
        print(f"✅ {result.get('message', '操作完成')}")
    elif command in ["delete", "del", "remove", "删除", "移除"] and args.stock:
        result = mx.manage(f"把{args.stock}从我的自选股列表删除")
        print(f"✅ {result.get('message', '操作完成')}")
    else:
        query = args.command
        if args.stock:
            query += " " + args.stock
        if any(kw in query for kw in ["查询", "列表", "我的自选", "有哪些"]):
            result = mx.query()
            format_query_result(result, output_dir)
        else:
            result = mx.manage(query)
            print(f"✅ {result.get('message', '操作完成')}")


if __name__ == "__main__":
    main()
