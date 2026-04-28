"""
market/service.py — 市场数据服务

编排数据抓取 + 标准化 + 存储 + 缓存 + 限流。
这是 market context 的唯一对外接口。
"""

from __future__ import annotations

import asyncio
from dataclasses import asdict
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
from hermes.market.adapters import is_hk_code
from hermes.strategy.models import MarketState

def _fetch_sina_intraday(codes: list[str]) -> dict[str, Optional[StockQuote]]:
    """
    新浪分时单股批量抓取（同步版，供 asyncio.to_thread 调用）。

    每个代码单独请求，不拉全市场。总耗时 ~1s * N只股票（并发）。
    """
    from datetime import date as _date
    import akshare as ak

    today = _date.today().strftime("%Y%m%d")
    result: dict[str, Optional[StockQuote]] = {code: None for code in codes}

    for code in codes:
        try:
            symbol = f"sh{code}" if code.startswith(("6", "9")) else f"sz{code}"
            df = ak.stock_intraday_sina(symbol=symbol, date=today)
            if df is None or df.empty:
                continue

            last = df.iloc[-1]
            price = float(last["price"])

            # prev_price 是分时前一笔价格，不能当昨收价用
            # 正确做法：用日K线获取昨收价来计算涨跌幅
            try:
                daily = ak.stock_zh_a_daily(symbol=symbol, adjust="qfq")
                prev_close = float(daily["close"].iloc[-2]) if len(daily) >= 2 else price
            except Exception:
                prev_close = price  # fallback
            change_pct = ((price - prev_close) / prev_close * 100) if prev_close > 0 else 0.0

            name = code
            if "name" in df.columns and len(df) > 0:
                name = str(df.iloc[0]["name"])

            result[code] = StockQuote(
                code=code,
                name=name,
                price=price,
                open=0.0,
                high=float(df["price"].max()),
                low=float(df["price"].min()),
                close=price,
                volume=int(df["volume"].sum()) if "volume" in df.columns else 0,
                amount=float(last["volume"]) * price if "volume" in df.columns else 0.0,
                change_pct=round(change_pct, 2),
            )
        except Exception:
            continue

    return result


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

            # 存储观测；即使本次抓取全量失败，也保留一次审计痕迹。
            if self._store and run_id:
                self._store.save_observation(
                    source="market_service",
                    kind="snapshot",
                    symbol=code,
                    payload={
                        "has_quote": quote is not None,
                        "has_technical": technical is not None,
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

    async def collect_intraday_batch(
        self,
        codes: list[dict],
        run_id: Optional[str] = None,
    ) -> list[StockSnapshot]:
        """
        盘中持仓监控专用轻量抓取。

        只获取实时行情（价格+涨跌幅+MA），不走全市场拉取。
        优先用新浪单股分时（~1s/只），次选东财单码，第三选日K线收盘价。
        """
        import asyncio
        import pandas as pd

        quotes: dict[str, StockQuote] = {}
        missing: list[str] = []
        names_by_code = {item["code"]: item.get("name", item["code"]) for item in codes}

        for item in codes:
            code = item["code"]
            cached = self._store.get_cached(code, "quote") if self._store else None
            if cached:
                quotes[code] = StockQuote(**cached)
            else:
                missing.append(code)

        if missing:
            # 方案A：新浪单股分时（最轻量，不拉全市场）
            sina_results = await asyncio.to_thread(_fetch_sina_intraday, missing)
            for code, quote in sina_results.items():
                if quote is not None:
                    quotes[code] = quote
                    missing.remove(code)

        if missing:
            # 方案B：东财全量快照过滤（可能很慢，跳过 tqdm 输出）
            for provider in self._market:
                if not missing:
                    break
                try:
                    import akshare as ak
                    df = ak.stock_zh_a_spot_em()
                    code_set = set(missing)
                    for _, row in df.iterrows():
                        code = str(row.get("代码", "")).strip()
                        if code not in code_set:
                            continue
                        code_set.discard(code)
                        quotes[code] = StockQuote(
                            code=code,
                            name=str(row.get("名称", "")),
                            price=float(row.get("最新价", 0) or 0),
                            open=float(row.get("今开", 0) or 0),
                            high=float(row.get("最高", 0) or 0),
                            low=float(row.get("最低", 0) or 0),
                            close=float(row.get("最新价", 0) or 0),
                            volume=int(row.get("成交量", 0) or 0),
                            amount=float(row.get("成交额", 0) or 0),
                            change_pct=float(row.get("涨跌幅", 0) or 0),
                        )
                        if code in missing:
                            missing.remove(code)
                except Exception:
                    pass

        if missing:
            # 方案C：日K线收盘价（仅能拿到收盘价，日内涨跌幅为0）
            for code in list(missing):
                for provider in self._iter_kline_providers(code):
                    try:
                        kline = await provider.get_kline(code, "daily", 2)
                        quote = self._quote_from_kline(code, kline)
                        if quote is not None:
                            quotes[code] = quote
                            missing.remove(code)
                            break
                    except Exception:
                        continue

        # 技术指标：MA20/60 用日K计算（并发，避免走 compute_technical_indicators 的慢路径）
        async def _tech_for(code: str) -> tuple[str, Optional[TechnicalIndicators]]:
            try:
                for provider in self._iter_kline_providers(code):
                    try:
                        kline = await provider.get_kline(code, "daily", 120)
                        if kline is None or kline.empty:
                            continue
                        closes = kline["close"].astype(float).tolist()
                        ma20 = float(pd.Series(closes).rolling(20).mean().iloc[-1]) if len(closes) >= 20 else 0.0
                        ma60 = float(pd.Series(closes).rolling(60).mean().iloc[-1]) if len(closes) >= 60 else 0.0
                        return code, TechnicalIndicators(
                            ma5=0, ma10=0, ma20=ma20, ma60=ma60,
                            ma120=0, ma250=0, rsi14=0, rsi28=0,
                            golden_cross=False, dead_cross=False,
                            volume_ratio=0, trend="",
                        )
                    except Exception:
                        continue
                return code, None
            except Exception:
                return code, None

        tech_results = await asyncio.gather(*[_tech_for(item["code"]) for item in codes])
        tech_by_code = {code: tech for code, tech in tech_results}

        snapshots: list[StockSnapshot] = []
        for item in codes:
            code = item["code"]
            quote = quotes.get(code)
            technical = tech_by_code.get(code)

            if self._store and quote is not None:
                payload = asdict(quote)
                if payload.get("timestamp") is not None:
                    payload["timestamp"] = payload["timestamp"].isoformat()
                self._store.save_observation(
                    source="market_service",
                    kind="quote",
                    symbol=code,
                    payload=payload,
                    run_id=run_id,
                )

            snapshots.append(StockSnapshot(
                code=code,
                name=names_by_code.get(code, quote.name if quote else code),
                quote=quote,
                technical=technical,
            ))

        return snapshots

    async def collect_market_state(
        self,
        run_id: Optional[str] = None,
    ) -> tuple[MarketState, dict]:
        """拉取指数数据 → 计算大盘信号。

        Returns:
            (MarketState, index_data) — index_data 含每个指数的原始行情，
            可用于写入 projection_market_state 表。
        """
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
                        "symbol": quote.symbol,
                        "price": quote.price,
                        "change_pct": quote.change_pct,
                        "ma20": quote.ma20,
                        "ma60": quote.ma60,
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

        return state, index_data

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

        for provider in self._iter_kline_providers(code):
            try:
                kline = await provider.get_kline(code, "daily", 2)
                quote = self._quote_from_kline(code, kline)
                if quote is not None:
                    return quote
            except Exception as e:
                _logger.info(f"[quote] {code} provider kline fallback failed: {e}")
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
        for provider in self._iter_kline_providers(code):
            try:
                kline = await provider.get_kline(code, "daily", 120)
                if kline is not None and not kline.empty:
                    return compute_technical_indicators(kline, quote)
            except Exception as e:
                _logger.info(f"[technical] {code} provider kline failed: {e}")
                continue
        return None

    def _iter_kline_providers(self, code: str):
        """为指定代码挑选合适的 K 线 provider。"""
        from hermes.market.adapters import AkShareHKMarketAdapter, is_hk_code

        want_hk = is_hk_code(code)
        for provider in self._market:
            if not hasattr(provider, "get_kline"):
                continue
            if want_hk and not isinstance(provider, AkShareHKMarketAdapter):
                continue
            if not want_hk and isinstance(provider, AkShareHKMarketAdapter):
                continue
            yield provider

    def _quote_from_kline(self, code: str, kline) -> Optional[StockQuote]:
        """从日 K 最后一根 bar 构造一个收盘快照。"""
        if kline is None or kline.empty:
            return None

        row = kline.iloc[-1]

        def _num(*keys: str, default: float = 0.0) -> float:
            for key in keys:
                value = row.get(key)
                if value is None:
                    continue
                try:
                    return float(value)
                except (TypeError, ValueError):
                    continue
            return default

        close = _num("close", "收盘")
        if close <= 0:
            return None

        return StockQuote(
            code=code,
            name=str(row.get("name") or row.get("名称") or row.get("证券名称") or code),
            price=close,
            open=_num("open", "开盘", default=close),
            high=_num("high", "最高", default=close),
            low=_num("low", "最低", default=close),
            close=close,
            volume=int(_num("volume", "成交量")),
            amount=_num("amount", "成交额"),
            change_pct=_num("涨跌幅", "pct_change"),
        )

    async def collect_sector_heatmap(self) -> list[dict]:
        """获取行业板块热力图数据（成交额前 AkShare）。"""
        for provider in self._market:
            if not hasattr(provider, "get_sector_heatmap"):
                continue
            try:
                data = await provider.get_sector_heatmap()
                if data:
                    return data
            except Exception as e:
                _logger.warning(f"[sector_heatmap] provider failed: {e}")
                continue
        return []
