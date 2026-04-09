#!/usr/bin/env python3
"""
mx_xuangu — 妙想智能选股

支持自然语言选股条件筛选：
  - 行情/财务指标筛选
  - 行业/板块/指数成分股筛选
  - 组合条件筛选

用法：
  python -m scripts.mx.mx_xuangu "今日涨幅大于2%的A股"
  python -m scripts.mx.mx_xuangu "净利润增长率大于30%的股票"
"""

import os
import sys
import json
import csv
import re
import argparse
import requests
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts.mx.client import MXBaseClient, _PROJECT_ROOT as ROOT
from scripts.utils.logger import get_logger

_logger = get_logger("mx.xuangu")
OUTPUT_DIR = ROOT / "data" / "mx_output"


def safe_filename(s: str, max_len: int = 80) -> str:
    s = re.sub(r'[<>:"/\\|?*]', "_", s)
    s = s.strip().replace(" ", "_")[:max_len]
    return s or "query"


def build_column_map(columns: List[Dict[str, Any]]) -> Dict[str, str]:
    name_map: Dict[str, str] = {}
    for col in columns or []:
        if not isinstance(col, dict):
            continue
        en_key = col.get("field", "") or col.get("name", "") or col.get("key", "")
        cn_name = col.get("displayName", "") or col.get("title", "") or col.get("label", "")
        date_msg = col.get('dateMsg', '')
        if date_msg:
            cn_name = cn_name + ' ' + date_msg
        if en_key is not None and cn_name is not None:
            name_map[str(en_key)] = str(cn_name)
    return name_map


def columns_order(columns: List[Dict[str, Any]]) -> List[str]:
    order: List[str] = []
    for col in columns or []:
        if not isinstance(col, dict):
            continue
        en_key = col.get("field") or col.get("name") or col.get("key")
        if en_key is not None:
            order.append(str(en_key))
    return order


def parse_partial_results_table(partial_results: str) -> List[Dict[str, str]]:
    if not partial_results or not isinstance(partial_results, str):
        return []
    lines = [ln.strip() for ln in partial_results.strip().splitlines() if ln.strip()]
    if not lines:
        return []

    def split_cells(line: str) -> List[str]:
        return [c.strip() for c in line.split("|") if c.strip() != ""]

    header_cells = split_cells(lines[0])
    if not header_cells:
        return []
    data_start = 1
    if data_start < len(lines) and re.match(r"^[\s\|\-]+$", lines[data_start]):
        data_start = 2
    rows: List[Dict[str, str]] = []
    for i in range(data_start, len(lines)):
        cells = split_cells(lines[i])
        if len(cells) < len(header_cells):
            cells.extend([""] * (len(header_cells) - len(cells)))
        else:
            cells = cells[:len(header_cells)]
        rows.append(dict(zip(header_cells, cells)))
    return rows


def datalist_to_rows(datalist, column_map, column_order):
    if not datalist:
        return []
    first = datalist[0]
    extra_keys = [k for k in first if k not in column_order]
    header_order = column_order + extra_keys
    rows = []
    for row in datalist:
        if not isinstance(row, dict):
            continue
        cn_row = {}
        for en_key in header_order:
            if en_key not in row:
                continue
            cn_name = column_map.get(en_key, en_key)
            val = row[en_key]
            if val is None:
                cn_row[cn_name] = ""
            elif isinstance(val, (dict, list)):
                cn_row[cn_name] = json.dumps(val, ensure_ascii=False)
            else:
                cn_row[cn_name] = str(val)
        rows.append(cn_row)
    return rows


class MXXuangu(MXBaseClient):
    """妙想智能选股客户端"""

    def search(self, query: str) -> Dict[str, Any]:
        """自然语言智能选股"""
        return self._post("/api/claw/stock-screen", {"keyword": query})

    @staticmethod
    def extract_data(result: Dict[str, Any]) -> Tuple[List[Dict[str, str]], str, Optional[str]]:
        status = result.get("status")
        if status != 0:
            return [], "", f"顶层错误: 状态码 {status} - {result.get('message', '')}"

        data = result.get("data", {})
        inner_data = data.get("data", {})

        data_list = inner_data.get("allResults", {}).get("result", {}).get("dataList", [])
        columns = inner_data.get("allResults", {}).get("result", {}).get("columns", [])

        if isinstance(data_list, list) and data_list:
            column_map = build_column_map(columns)
            order = columns_order(columns)
            rows = datalist_to_rows(data_list, column_map, order)
            return rows, "dataList", None

        partial_results = inner_data.get("partialResults", "")
        if partial_results:
            rows = parse_partial_results_table(partial_results)
            return rows, "partialResults", None

        return [], "", "返回中无有效 dataList 且 partialResults 无法解析或为空"


def main():
    parser = argparse.ArgumentParser(description='妙想智能选股')
    parser.add_argument('query', nargs='?', help='自然语言查询')
    parser.add_argument('--query', dest='query_opt', help='自然语言查询（显式参数）')
    parser.add_argument('--output-dir', dest='output_dir', help=f'输出目录，默认 {OUTPUT_DIR}')
    args = parser.parse_args()

    query = args.query_opt or args.query
    if not query:
        parser.print_help()
        sys.exit(1)

    output_dir = Path(args.output_dir) if args.output_dir else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        mx = MXXuangu()
        result = mx.search(query)
        rows, data_source, err = mx.extract_data(result)

        if err:
            print(f"错误: {err}")
            sys.exit(2)

        if not rows:
            print("未找到符合条件的数据")
            sys.exit(0)

        fieldnames = list(rows[0].keys())
        safe_name = safe_filename(query)
        csv_path = output_dir / f"mx_xuangu_{safe_name}.csv"
        desc_path = output_dir / f"mx_xuangu_{safe_name}_description.txt"

        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

        description_lines = [
            "智能选股结果说明",
            "=" * 40,
            f"查询内容: {query}",
            f"数据行数: {len(rows)}（来源: {data_source}）",
            f"列名: {', '.join(fieldnames)}",
        ]
        desc_path.write_text("\n".join(description_lines), encoding="utf-8")

        print(f"✅ CSV: {csv_path}")
        print(f"📄 描述: {desc_path}")
        print(f"📊 行数: {len(rows)}")

        json_path = output_dir / f"mx_xuangu_{safe_name}_raw.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"📄 原始JSON: {json_path}")

    except Exception as e:
        print(f"错误: {str(e)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
