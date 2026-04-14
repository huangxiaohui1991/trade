"""
market/mx_async.py — 妙想 API 异步客户端

使用 httpx AsyncClient 替代 V1 的 requests 同步调用。
用于 MarketService 的 MX adapter 链路。
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

try:
    import httpx
except ImportError:
    httpx = None


def _load_apikey() -> str:
    """从环境变量或 .env 加载 MX_APIKEY。"""
    key = os.environ.get("MX_APIKEY", "").strip()
    if key:
        return key

    env_path = Path(__file__).resolve().parent.parent.parent.parent / ".env"
    if env_path.exists():
        with open(env_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("MX_APIKEY="):
                    return line.split("=", 1)[1].strip()
    return ""


BASE_URL = "https://mkapi2.dfcfs.com/finskillshub"


class MXAsyncClient:
    """妙想 API 异步客户端。"""

    def __init__(self, api_key: Optional[str] = None, timeout: float = 20.0):
        if httpx is None:
            raise ImportError("httpx is required: pip install httpx")
        self._api_key = api_key or _load_apikey()
        self._timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None

    async def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=BASE_URL,
                timeout=self._timeout,
                headers={
                    "Content-Type": "application/json",
                    "apikey": self._api_key,
                },
            )
        return self._client

    async def post(self, endpoint: str, data: dict[str, Any]) -> dict[str, Any]:
        """POST 请求，返回 JSON dict。"""
        client = await self._ensure_client()
        try:
            resp = await client.post(endpoint, json=data)
            resp.raise_for_status()
            return resp.json()
        except httpx.TimeoutException:
            return {"error": "timeout", "status": -1}
        except httpx.HTTPStatusError as e:
            return {"error": f"HTTP {e.response.status_code}", "status": e.response.status_code}
        except Exception as e:
            return {"error": str(e), "status": -1}

    async def search_stocks(self, query: str) -> dict[str, Any]:
        """智能选股。"""
        return await self.post("/api/claw/stock-screen", {"keyword": query})

    async def search_news(self, query: str) -> dict[str, Any]:
        """资讯搜索。"""
        return await self.post("/api/claw/search", {"keyword": query})

    async def query_data(self, query: str) -> dict[str, Any]:
        """数据查询。"""
        return await self.post("/api/claw/data-query", {"keyword": query})

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None
