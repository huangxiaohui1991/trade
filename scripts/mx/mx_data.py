#!/usr/bin/env python3
"""
mx_data — 妙想金融数据查询

基于东方财富权威数据库，支持自然语言查询：
  - 行情数据（实时/历史）
  - 财务数据（ROE/营收/利润）
  - 关系与经营数据

用法：
  python -m scripts.mx.mx_data "东方财富最新价"
  python -m scripts.mx.mx_data "贵州茅台近三年净利润"
"""

import hashlib
import os
import sys
import json
import re
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts.mx.client import MXBaseClient, _PROJECT_ROOT as ROOT
from scripts.utils.logger import get_logger
from scripts.utils.cache import load_json_cache, save_json_cache

_logger = get_logger("mx.data")
OUTPUT_DIR = ROOT / "data" / "mx_output"


def safe_filename(s: str, max_len: int = 80) -> str:
    s = re.sub(r'[<>:"/\\|?*\[\]]', "_", s)
    s = s.strip().replace(" ", "_")[:max_len]
    return s or "query"


def flatten_value(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)


def ordered_keys(table: Dict[str, Any], indicator_order: List[Any]) -> List[Any]:
    data_keys = [k for k in table.keys() if k != "headName"]
    key_map = {str(k): k for k in data_keys}
    preferred: List[Any] = []
    seen: set = set()
    for key in indicator_order:
        key_str = str(key)
        if key_str in key_map and key_str not in seen:
            preferred.append(key_map[key_str])
            seen.add(key_str)
    for key in data_keys:
        key_str = str(key)
        if key_str not in seen:
            preferred.append(key)
            seen.add(key_str)
    return preferred


def normalize_values(raw_values: List[Any], expected_len: int) -> List[str]:
    values = [flatten_value(v) for v in raw_values]
    if len(values) < expected_len:
        values.extend([""] * (expected_len - len(values)))
    return values[:expected_len]


def return_code_map(block: Dict[str, Any]) -> Dict[str, str]:
    for key in ("returnCodeMap", "returnCodeNameMap", "codeMap"):
        data = block.get(key)
        if isinstance(data, dict):
            return {str(k): flatten_value(v) for k, v in data.items()}
    return {}


def format_indicator_label(key: str, name_map: Dict[str, Any], code_map: Dict[str, str]) -> str:
    mapped = name_map.get(key)
    if mapped is None and key.isdigit():
        mapped = name_map.get(int(key))
    if mapped not in (None, ""):
        return flatten_value(mapped)
    mapped_code = code_map.get(key)
    if mapped_code not in (None, ""):
        return flatten_value(mapped_code)
    if key.isdigit():
        return ""
    return key


def table_to_rows(block: Dict[str, Any]) -> Tuple[List[Dict[str, str]], List[str]]:
    table = block.get("table") or {}
    name_map = block.get("nameMap") or {}
    if isinstance(name_map, list):
        name_map = {str(i): v for i, v in enumerate(name_map)}
    elif not isinstance(name_map, dict):
        name_map = {}

    if not isinstance(table, dict):
        if isinstance(table, list):
            if not table:
                return [], []
            if isinstance(table[0], dict):
                rows = table
            else:
                rows = [
                    dict(zip([f"column_{i}" for i in range(len(table[0]))], row))
                    for row in table
                ]
        else:
            return [], []
        return [{name_map.get(k, k): flatten_value(v) for k, v in row.items()} for row in rows], list(rows[0].keys())

    headers = table.get("headName") or []
    if not isinstance(headers, list):
        headers = []
    order = ordered_keys(table, block.get("indicatorOrder") or [])
    entity_name = flatten_value(block.get("entityName") or "") or "指标"
    code_map = return_code_map(block)

    rows: List[Dict[str, str]] = []
    data_key_count = len([key for key in table.keys() if key != "headName"])

    if len(headers) > 0:
        fieldnames = ["date"]
        for key in order:
            if key != "headName":
                label = format_indicator_label(str(key), name_map, code_map)
                if label:
                    fieldnames.append(label)
        for row_idx, date in enumerate(headers):
            row = {"date": flatten_value(date)}
            for key in order:
                if key == "headName":
                    continue
                label = format_indicator_label(str(key), name_map, code_map)
                if not label:
                    continue
                raw_values = table.get(key, [])
                value = raw_values[row_idx] if row_idx < len(raw_values) else ""
                row[label] = flatten_value(value)
            rows.append(row)
        return rows, fieldnames

    if len(headers) == 1 and data_key_count >= 1:
        fieldnames = [entity_name, flatten_value(headers[0])]
        for key in order:
            raw_values = table.get(key, [])
            value = raw_values[0] if isinstance(raw_values, list) and raw_values else raw_values
            label = format_indicator_label(str(key), name_map, code_map)
            rows.append({fieldnames[0]: label, fieldnames[1]: flatten_value(value)})
        return rows, fieldnames

    return [], []


class MXData(MXBaseClient):
    """妙想金融数据查询客户端"""

    def query(self, tool_query: str) -> Dict[str, Any]:
        """自然语言查询金融数据"""
        return self._post("/api/claw/query", {"toolQuery": tool_query})

    @staticmethod
    def parse_result(result: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[str], int, Optional[str]]:
        status = result.get("status")
        message = result.get("message", "")
        if status != 0:
            return [], [], 0, f"顶层错误: 状态码 {status} - {message}"

        data = result.get("data", {})
        inner_data = data.get("data", {})
        search_result = inner_data.get("searchDataResultDTO", {})
        dto_list = search_result.get("dataTableDTOList", [])

        if not dto_list:
            return [], [], 0, "接口返回中无 dataTableDTOList"

        condition_parts: List[str] = []
        tables: List[Dict[str, Any]] = []
        total_rows = 0

        for i, dto in enumerate(dto_list):
            if not isinstance(dto, dict):
                continue
            sheet_name = safe_filename(
                dto.get("title") or dto.get("inputTitle") or dto.get("entityName") or f"表{i + 1}"
            )
            condition = dto.get("condition")
            if condition is not None and condition != "":
                entity = dto.get("entityName") or sheet_name
                condition_parts.append(f"[{entity}]\n{condition}")

            rows, fieldnames = table_to_rows(dto)
            if not rows:
                continue
            tables.append({"sheet_name": sheet_name, "rows": rows, "fieldnames": fieldnames})
            total_rows += len(rows)

        if not tables:
            return [], condition_parts, 0, "dataTableDTOList 中无有效 table 数据"
        return tables, condition_parts, total_rows, None

    @staticmethod
    def format_terminal(result: Dict[str, Any], tables: List[Dict[str, Any]], total_rows: int) -> str:
        output = []
        status = result.get("status")
        message = result.get("message", "")
        if status != 0:
            output.append(f"**错误**: 状态码 {status} - {message}")
            return "\n".join(output)

        data = result.get("data", {})
        inner_data = data.get("data", {})
        search_result = inner_data.get("searchDataResultDTO", {})
        entity_tags = search_result.get("entityTagDTOList", [])

        if entity_tags:
            output.append("**查询证券**:")
            entities = []
            for tag in entity_tags:
                name = tag.get("fullName", "")
                code = tag.get("secuCode", "")
                type_name = tag.get("entityTypeName", "")
                entities.append(f"- {name} ({code}) - {type_name}")
            output.append("\n".join(entities))
            output.append("")

        output.append(f"**查询结果**: {len(tables)} 个表，共 {total_rows} 行数据\n")

        if tables:
            first = tables[0]
            output.append(f"**{first['sheet_name']}** (前20行预览):\n")
            rows = first["rows"][:20]
            if rows:
                fieldnames = first["fieldnames"]
                output.append("| " + " | ".join(fieldnames) + " |")
                output.append("| " + " | ".join(["---"] * len(fieldnames)) + " |")
                for row in rows:
                    cells = [str(row.get(f, "")) for f in fieldnames]
                    output.append("| " + " | ".join(cells) + " |")

        return "\n".join(output)

    @staticmethod
    def write_output_files(
        query_text: str,
        output_dir: Path,
        tables: List[Dict[str, Any]],
        total_rows: int,
        condition_parts: List[str],
    ) -> Tuple[Path, Path]:
        output_dir.mkdir(parents=True, exist_ok=True)
        safe_name = safe_filename(query_text)
        file_path = output_dir / f"mx_data_{safe_name}.xlsx"
        desc_path = output_dir / f"mx_data_{safe_name}_description.txt"

        with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
            for table in tables:
                df = pd.DataFrame(table["rows"], columns=table["fieldnames"])
                df.to_excel(writer, sheet_name=table["sheet_name"], index=False)

        description_lines = [
            "金融数据查询结果说明",
            "=" * 40,
            f"查询内容: {query_text}",
            f"数据文件路径: {file_path}",
            f"数据行数: {total_rows}",
            f"表数量: {len(tables)}",
            f"Sheet 列表: {', '.join([t['sheet_name'] for t in tables])}",
        ]
        if condition_parts:
            description_lines.append("")
            description_lines.append("筛选条件:")
            description_lines.extend(condition_parts)

        desc_path.write_text("\n".join(description_lines), encoding="utf-8")
        return file_path, desc_path


# ---------------------------------------------------------------------------
# MX Data 缓存 TTL（秒）
# ---------------------------------------------------------------------------
# 股票名称：几乎不变，缓存 24h
TTL_NAME = 24 * 3600
# 日线历史数据：日频变化，收盘后有效，缓存 6h
TTL_HIST = 6 * 3600
# 基本面（ROE/营收/现金流）：季频，缓存 4h
TTL_FINANCIAL = 4 * 3600
# 资金流向：日内可能变化，缓存 2h
TTL_FLOW = 2 * 3600
# 实时行情：分钟级，缓存 1h
TTL_REALTIME = 1 * 3600


# ---------------------------------------------------------------------------
# 缓存查询辅助函数
# ---------------------------------------------------------------------------

def _query_cache_key(query: str) -> str:
    """生成查询字符串的 MD5 哈希作为缓存 key（避免文件系统路径问题）"""
    return hashlib.md5(query.strip().encode("utf-8")).hexdigest()[:32]


def _cached_mx_query(query: str, ttl_seconds: int) -> Dict[str, Any]:
    """
    带缓存的 mx_data 查询。

    读缓存 → 命中则返回；未命中则调 API → 成功写入缓存后返回。
    失败返回空 dict（status != 0），使调用方 parse_result 自然走 error 分支。

    Args:
        query: 妙想查询语句
        ttl_seconds: 缓存有效期

    Returns:
        API 返回的 dict；失败或缓存未命中返回 {"status": -1, "message": str}
    """
    cache_key = _query_cache_key(query)
    cached = load_json_cache("mx_data", cache_key, max_age_seconds=ttl_seconds)
    if cached is not None:
        _logger.debug(f"[mx_data cache hit] ttl={ttl_seconds}s key={cache_key[:8]} query={query[:40]}")
        return cached.get("data", {})

    try:
        mx = MXData()
        result = mx.query(query)
        save_json_cache("mx_data", cache_key, result)
        _logger.info(f"[mx_data cache miss→saved] ttl={ttl_seconds}s key={cache_key[:8]} query={query[:40]}")
        return result
    except Exception as e:
        _logger.debug(f"[mx_data cache miss→api failed] {e}")
        return {"status": -1, "message": str(e)}


def main():
    if len(sys.argv) < 2:
        print(f"用法: python -m scripts.mx.mx_data \"查询问句\" [输出目录]")
        print(f"默认输出目录: {OUTPUT_DIR}")
        print("示例: python -m scripts.mx.mx_data \"东方财富最新价\"")
        sys.exit(1)

    if len(sys.argv) >= 3:
        query = " ".join(sys.argv[1:-1])
        output_dir = Path(sys.argv[-1])
    else:
        query = " ".join(sys.argv[1:])
        output_dir = OUTPUT_DIR

    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        mx = MXData()
        result = mx.query(query)
        tables, condition_parts, total_rows, err = mx.parse_result(result)

        if err:
            print(f"错误: {err}")
            sys.exit(1)

        print(mx.format_terminal(result, tables, total_rows))

        file_path, desc_path = mx.write_output_files(query, output_dir, tables, total_rows, condition_parts)
        print(f"\n✅ Excel 文件: {file_path}")
        print(f"📄 描述文件: {desc_path}")
        print(f"📊 总行数: {total_rows}, 表数: {len(tables)}")

        json_filename = output_dir / f"mx_data_{safe_filename(query)}_raw.json"
        with open(json_filename, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"📄 原始JSON: {json_filename}")

    except Exception as e:
        print(f"错误: {str(e)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
