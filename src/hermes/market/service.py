"""
market/service.py — 市场数据服务

编排数据抓取 + 标准化 + 存储 + 缓存 + 限流。
这是 market context 的唯一对外接口。
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from hermes.market.models import (
    FundFlow,
    SentimentData,
    StockQuote,
    StockSnapshot,
    TechnicalIndicators,
)
from hermes.market.store import MarketStore
from hermes.market.indicators import compute_technical_indicators
from hermes.strategy.models import MarketState

_logger = logging.getLogger(__name__)


class MarketService:
    """编排数据抓取，自动 fallback + 缓存 + 限流。"""

    def __init__(
        self,
        market_providers: list = None,
        financial_providers: list = None,
        flow_providers: list = None,
        sentiment_providers: list = None,
        store: Optional[MarketStore] = None,
        concurrency: int = 5,
    ):
        self._market = market_providers or []
        self._financial = financial_providers or []
        self._flow = flow_providers or []
        self._sentiment = sentiment_providers or []
        self._store = store
        self._sem = asyncio.Semaphore(concurrency)

    async def collect_snapshot(
        self,
        code: str,
        name: str = "",
        run_id: Optional[str] = None,
    ) -> StockSnapshot:
        """
        抓取单股全部数据 → 组装 StockSnapshot。

        五个维度并发获取，自动 fallback，追加到 market_observations。
        """
        async with self._sem:
            quote_task = self._get_quote(code)
            fin_task = self._get_financial(code)
            flow_task = self._get_flow(code)
            sent_task = self._get_sentiment(code, name)

            quote, fin, flow, sent = await asyncio.gather(
                quote_task, fin_task, flow_task, sent_task,
                return_exceptions=True,
            )

            # 技术指标：从 K 线计算
            technical = await self._get_technical(code, quote)

            # 异常处理
            if isinstance(quote, Exception):
                _logger.warning(f"[collect] {code} quote failed: {quote}")
                quote = None
            if isinstance(fin, Exception):
                _logger.warning(f"[collect] {code} financial failed: {fin}")
                fin = None
            if isinstance(flow, Exception):
                _logger.warning(f"[collect] {code} flow failed: {flow}")
                flow = None
            if isinstance(sent, Exception):
                _logger.warning(f"[collect] {code} sentiment failed: {sent}")
                sent = None

            snapshot = StockSnapshot(
                code=code,
                name=name or (quote.name if quote else code),
                quote=quote,
                technical=technical,
                financial=fin,
                flow=flow,
                sentiment=sent,
            )

            # 存储观测（只存有效的观测，不存全 None 状态）
            if self._store and run_id:
                has_any = (quote is not None or fin is not None
                           or flow is not None or sent is not None
                           or technical is not None)
                if has_any:
                    self._store.save_observation(
                        source="market_service",
                        kind="snapshot",
                        symbol=code,
                        payload={
                            "has_quote": quote is not None,
                            "has_financial": fin is not None,
                            "has_flow": flow is not None,
                            "has_sentiment": sent is not None,
                        },
                        run_id=run_id,
                    )

            return snapshot

    async def collect_batch(
        self,
        codes: list[dict],
        run_id: Optional[str] = None,
    ) -> list[StockSnapshot]:
        """
        批量抓取（受 semaphore 限流）。

        Args:
            codes: [{"code": "002138", "name": "双环传动"}, ...]
        """
        tasks = [
            self.collect_snapshot(item["code"], item.get("name", ""), run_id)
            for item in codes
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        snapshots = []
        for i, r in enumerate(results):
            if isinstance(r, Exception):
                _logger.error(f"[batch] {codes[i]['code']} failed: {r}")
                snapshots.append(StockSnapshot(
                    code=codes[i]["code"],
                    name=codes[i].get("name", codes[i]["code"]),
                ))
            else:
                snapshots.append(r)

        return snapshots

    async def collect_market_state(
        self,
        run_id: Optional[str] = None,
    ) -> MarketState:
        """拉取指数数据 → 计算大盘信号。"""
        from hermes.strategy.timer import compute_market_signal

        index_data = {}
        for provider in self._market:
            try:
                indices = await provider.get_index([
                    "sh000001", "sz399001", "sz399006",
                ])
                for sym, quote in indices.items():
                    name = quote.name
                    index_data[name] = {
                        "price": quote.price,
                        "change_pct": quote.change_pct,
                        "above_ma20": quote.above_ma20,
                        "below_ma60_days": quote.below_ma60_days,
                    }
                if index_data:
                    break
            except Exception as e:
                _logger.warning(f"[market_state] provider failed: {e}")
                continue

        state = compute_market_signal(index_data)

        if self._store and run_id:
            self._store.save_observation(
                source="market_service",
                kind="market_state",
                symbol="market",
                payload={
                    "signal": state.signal.value,
                    "multiplier": state.multiplier,
                },
                run_id=run_id,
            )

        return state

    # ------------------------------------------------------------------
    # 内部：带 fallback 的数据获取
    # ------------------------------------------------------------------

    async def _get_quote(self, code: str) -> Optional[StockQuote]:
        """从 market providers 获取行情，自动 fallback。"""
        # 先检查缓存
        if self._store:
            cached = self._store.get_cached(code, "quote")
            if cached:
                return StockQuote(**cached)

        for provider in self._market:
            try:
                quotes = await provider.get_realtime([code])
                if code in quotes:
                    return quotes[code]
            except Exception as e:
                _logger.info(f"[quote] {code} provider failed: {e}")
                continue
        return None

    async def _get_financial(self, code: str) -> Optional[object]:
        """从 financial providers 获取财务数据，自动 fallback。"""
        if self._store:
            cached = self._store.get_cached(code, "financial")
            if cached:
                from hermes.market.models import FinancialReport
                return FinancialReport(**cached)

        for provider in self._financial:
            try:
                result = await provider.get_financial(code)
                if result is not None:
                    return result
            except Exception as e:
                _logger.info(f"[financial] {code} provider failed: {e}")
                continue
        return None

    async def _get_flow(self, code: str) -> Optional[FundFlow]:
        """从 flow providers 获取资金流向，自动 fallback。"""
        # 注意：flow 的 save_observation 只存了 has_flow 布尔标记，不含实际数据，
        # 所以不走缓存，直接从 provider 拉取。
        for provider in self._flow:
            try:
                result = await provider.get_fund_flow(code)
                if result is not None:
                    return result
            except Exception as e:
                _logger.info(f"[flow] {code} provider failed: {e}")
                continue
        return None

    async def _get_sentiment(self, code: str, name: str) -> Optional[SentimentData]:
        """从 sentiment providers 获取舆情，自动 fallback。"""
        if self._store:
            cached = self._store.get_cached(code, "sentiment")
            if cached:
                return SentimentData(**cached)

        for provider in self._sentiment:
            try:
                result = await provider.search_news(f"{name} 最新研报")
                if result is not None:
                    return result
            except Exception as e:
                _logger.info(f"[sentiment] {code} provider failed: {e}")
                continue
        return None

    async def _get_technical(self, code: str, quote: Optional[StockQuote]) -> Optional[TechnicalIndicators]:
        """从 K 线计算技术指标。"""
        # 优先用 market providers 的 K 线
        for provider in self._market:
            if not hasattr(provider, "get_kline"):
                continue
            try:
                kline = await provider.get_kline(code, "daily", 120)
                if kline is not None and not kline.empty:
                    return compute_technical_indicators(kline, quote)
            except Exception as e:
                _logger.info(f"[technical] {code} provider kline failed: {e}")
                continue
        return None
