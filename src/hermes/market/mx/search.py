"""
market/mx/search.py — 妙想资讯搜索

从 V1 scripts/mx/mx_search.py 迁移，去掉 CLI 入口和文件输出。
"""

from __future__ import annotations

import json
from typing import Any, Dict

from hermes.market.mx.client import MXBaseClient


class MXSearch(MXBaseClient):
    """妙想资讯搜索客户端。"""

    def search(self, query: str) -> Dict[str, Any]:
        return self._post("/api/claw/news-search", {"query": query})

    @staticmethod
    def extract_items(result: Dict[str, Any]) -> list[dict]:
        """提取搜索结果列表。"""
        data = result.get("data", {})
        inner = data.get("data", {})
        search_resp = inner.get("llmSearchResponse", {})
        return search_resp.get("data", [])
