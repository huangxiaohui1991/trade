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
        return await self.post("/api/claw/news-search", {"query": query})

    async def query_data(self, query: str) -> dict[str, Any]:
        """数据查询。"""
        return await self.post("/api/claw/query", {"toolQuery": query})

    # ------------------------------------------------------------------
    # 自选股管理
    # ------------------------------------------------------------------

    async def get_self_select(self) -> dict[str, Any]:
        """查询自选股列表。"""
        return await self.post("/api/claw/self-select/get", {})

    async def manage_self_select(self, query: str) -> dict[str, Any]:
        """管理自选股（添加/删除，自然语言）。"""
        return await self.post("/api/claw/self-select/manage", {"query": query})

    # ------------------------------------------------------------------
    # 模拟交易
    # ------------------------------------------------------------------

    async def mock_positions(self) -> dict[str, Any]:
        """查询模拟持仓。"""
        return await self.post("/api/claw/mockTrading/positions", {"moneyUnit": 1})

    async def mock_balance(self) -> dict[str, Any]:
        """查询模拟账户资金。"""
        return await self.post("/api/claw/mockTrading/balance", {"moneyUnit": 1})

    async def mock_orders(self) -> dict[str, Any]:
        """查询模拟委托记录。"""
        return await self.post("/api/claw/mockTrading/orders", {"fltOrderDrt": 0, "fltOrderStatus": 0})

    async def mock_trade(
        self, trade_type: str, stock_code: str, quantity: int,
        price: float | None = None, use_market_price: bool = False,
    ) -> dict[str, Any]:
        """模拟买入/卖出。"""
        body: dict[str, Any] = {
            "type": trade_type,
            "stockCode": stock_code,
            "quantity": quantity,
            "useMarketPrice": use_market_price,
        }
        if not use_market_price and price is not None:
            body["price"] = price
        return await self.post("/api/claw/mockTrading/trade", body)

    async def mock_cancel(self, order_id: str | None = None, cancel_all: bool = False) -> dict[str, Any]:
        """模拟撤单。"""
        if cancel_all:
            body: dict[str, Any] = {"type": "all"}
        else:
            body = {"type": "order", "orderId": order_id or ""}
        return await self.post("/api/claw/mockTrading/cancel", body)

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None
