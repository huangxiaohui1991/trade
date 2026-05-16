"""Market data provider protocols."""

from __future__ import annotations

from typing import Optional, Protocol, runtime_checkable

import pandas as pd

from astock_trading.market.models import (
    FinancialReport,
    FundFlow,
    IndexQuote,
    SentimentData,
    StockQuote,
)

class MarketDataProvider(Protocol):
    """行情数据源。"""

    async def get_realtime(self, codes: list[str]) -> dict[str, StockQuote]:
        """批量获取实时行情。"""
        ...

    async def get_kline(self, code: str, period: str, count: int) -> Optional[pd.DataFrame]:
        """获取 K 线数据。"""
        ...

    async def get_index(self, symbols: list[str]) -> dict[str, IndexQuote]:
        """获取指数行情。"""
        ...


@runtime_checkable
class FinancialDataProvider(Protocol):
    """财务数据源。"""

    async def get_financial(self, code: str) -> Optional[FinancialReport]:
        """获取财务数据。"""
        ...


@runtime_checkable
class FlowDataProvider(Protocol):
    """资金流向数据源。"""

    async def get_fund_flow(self, code: str, days: int) -> Optional[FundFlow]:
        """获取资金流向。"""
        ...


@runtime_checkable
class SentimentProvider(Protocol):
    """舆情数据源。"""

    async def search_news(self, query: str) -> Optional[SentimentData]:
        """搜索新闻/研报。"""
        ...


@runtime_checkable
class ScreenerProvider(Protocol):
    """选股数据源。"""

    async def search_stocks(self, query: str) -> list[dict]:
        """选股筛选。"""
        ...
