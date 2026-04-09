#!/usr/bin/env python3
"""
mx_search — 妙想资讯搜索

基于东方财富妙想搜索能力，金融场景信源智能筛选：
  - 研报、新闻、公告、政策解读
  - 个股/板块/宏观资讯

用法：
  python -m scripts.mx.mx_search "贵州茅台最新研报"
  python -m scripts.mx.mx_search "人工智能板块近期新闻"
"""

import os
import sys
import json
import re
import requests
from pathlib import Path
from typing import Dict, List, Optional, Any

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts.mx.client import MXBaseClient, _PROJECT_ROOT as ROOT
from scripts.utils.logger import get_logger

_logger = get_logger("mx.search")
OUTPUT_DIR = ROOT / "data" / "mx_output"


def safe_filename(text: str, max_len: int = 80) -> str:
    cleaned = re.sub(r'[<>:"/\\|?*]', "_", text).strip().replace(" ", "_")
    return (cleaned[:max_len] or "query").strip("._")


class MXSearch(MXBaseClient):
    """妙想资讯搜索客户端"""

    def search(self, query: str) -> Dict[str, Any]:
        """搜索金融资讯"""
        return self._post("/api/claw/news-search", {"query": query})

    @staticmethod
    def extract_content(result: Dict[str, Any]) -> str:
        def _extract(raw: Any) -> str:
            if not isinstance(raw, dict):
                if isinstance(raw, str):
                    return raw.strip()
                return ""
            for wrapper_key in ("data", "result"):
                wrapped = raw.get(wrapper_key)
                if isinstance(wrapped, dict):
                    nested = _extract(wrapped)
                    if nested:
                        return nested
            for key in ("llmSearchResponse", "searchResponse", "content", "answer", "summary"):
                value = raw.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
                if isinstance(value, (list, dict)):
                    return json.dumps(value, ensure_ascii=False, indent=2)
            return json.dumps(raw, ensure_ascii=False, indent=2)
        return _extract(result)

    @staticmethod
    def format_pretty(result: Dict[str, Any]) -> str:
        output = []
        status = result.get("status")
        message = result.get("message", "")
        if status != 0:
            output.append(f"错误: 状态码 {status} - {message}")
            return "\n".join(output)

        data = result.get("data", {})
        inner_data = data.get("data", {})
        search_response = inner_data.get("llmSearchResponse", {})
        items = search_response.get("data", [])

        if not items:
            return "未找到相关资讯"

        output.append(f"搜索结果: 共找到 {len(items)} 条相关资讯:\n")

        for i, item in enumerate(items, 1):
            title = item.get("title", "无标题")
            content = item.get("content", "无内容")
            date = item.get("date", "")
            ins_name = item.get("insName", "")
            info_type = item.get("informationType", "")
            rating = item.get("rating", "")
            entity_name = item.get("entityFullName", "")

            type_map = {"REPORT": "研报", "NEWS": "新闻", "ANNOUNCEMENT": "公告"}
            type_cn = type_map.get(info_type, info_type)

            output.append(f"--- {i}. {title} ---")
            meta = []
            if entity_name:
                meta.append(f"证券: {entity_name}")
            if ins_name:
                meta.append(f"机构: {ins_name}")
            if date:
                meta.append(f"日期: {date.split()[0]}")
            if type_cn:
                meta.append(f"类型: {type_cn}")
            if rating:
                meta.append(f"评级: {rating}")
            if meta:
                output.append(" | ".join(meta))
            if content:
                output.append("")
                output.append(content)
            output.append("")

        return "\n".join(output)


def main():
    if len(sys.argv) < 2:
        print(f"用法: python -m scripts.mx.mx_search \"搜索问句\" [输出目录]")
        print(f"默认输出目录: {OUTPUT_DIR}")
        sys.exit(1)

    if len(sys.argv) >= 3:
        query = " ".join(sys.argv[1:-1])
        output_dir = Path(sys.argv[-1])
    else:
        query = " ".join(sys.argv[1:])
        output_dir = OUTPUT_DIR

    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        mx = MXSearch()
        result = mx.search(query)
        print(mx.format_pretty(result))

        content = mx.extract_content(result)
        if content.strip():
            filename = output_dir / f"mx_search_{safe_filename(query)}.txt"
            with open(filename, "w", encoding="utf-8") as f:
                f.write(content)
            print(f"\n✅ 纯文本结果: {filename}")

        json_filename = output_dir / f"mx_search_{safe_filename(query)}.json"
        with open(json_filename, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"📄 原始JSON: {json_filename}")

    except Exception as e:
        print(f"错误: {str(e)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
