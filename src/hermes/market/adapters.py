"""
market/adapters.py — 数据源 Protocol + adapter 实现

Protocol 定义标准接口，adapter 实现具体数据源。
所有 adapter 返回标准化的 dataclass，不暴露数据源细节。
"""

from __future__ import annotations

import asyncio
from typing import Optional, Protocol, runtime_checkable

import pandas as pd

from hermes.market.models import (
    FinancialReport,
    FundFlow,
    IndexQuote,
    SentimentData,
    StockQuote,
    TechnicalIndicators,
)


# ---------------------------------------------------------------------------
# Protocol 接口
# ---------------------------------------------------------------------------

@runtime_checkable
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


# ---------------------------------------------------------------------------
# AkShare Adapters（同步 akshare 用 asyncio.to_thread 包装）
# ---------------------------------------------------------------------------

class AkShareMarketAdapter:
    """AkShare 行情 adapter。"""

    async def get_realtime(self, codes: list[str]) -> dict[str, StockQuote]:
        return await asyncio.to_thread(self._get_realtime_sync, codes)

    async def get_kline(self, code: str, period: str = "daily", count: int = 120) -> Optional[pd.DataFrame]:
        return await asyncio.to_thread(self._get_kline_sync, code, period, count)

    async def get_index(self, symbols: list[str]) -> dict[str, IndexQuote]:
        return await asyncio.to_thread(self._get_index_sync, symbols)

    def _get_realtime_sync(self, codes: list[str]) -> dict[str, StockQuote]:
        try:
            import akshare as ak
            df = ak.stock_zh_a_spot_em()
            if df is None or df.empty:
                return {}

            code_set = set(codes)
            result = {}
            for _, row in df.iterrows():
                code = str(row.get("代码", "")).strip()
                if code not in code_set:
                    continue
                result[code] = StockQuote(
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
            return result
        except Exception:
            return {}

    def _get_kline_sync(self, code: str, period: str, count: int) -> Optional[pd.DataFrame]:
        try:
            import akshare as ak
            from datetime import datetime, timedelta

            # 适配 akshare 接口格式：沪市加 sh，深市加 sz
            if code.startswith(("6", "9")):
                symbol = f"sh{code}"
            else:
                symbol = f"sz{code}"

            df = ak.stock_zh_a_daily(symbol=symbol, adjust="qfq")
            if df is None or df.empty:
                return None

            # 按日期升序，取最近足够多数据
            df = df.sort_values("date").tail(count * 2).reset_index(drop=True)

            # 添加涨跌幅列（用 close 计算）
            df["涨跌幅"] = df["close"].pct_change() * 100

            return df
        except Exception:
            return None

    def _get_index_sync(self, symbols: list[str]) -> dict[str, IndexQuote]:
        # 指数行情需要单独实现，暂返回空
        return {}


class AkShareFinancialAdapter:
    """AkShare 财务 adapter。"""

    async def get_financial(self, code: str) -> Optional[FinancialReport]:
        return await asyncio.to_thread(self._get_financial_sync, code)

    def _get_financial_sync(self, code: str) -> Optional[FinancialReport]:
        try:
            import akshare as ak
            df = ak.stock_financial_analysis_indicator(symbol=code, start_year="2024")
            if df is None or df.empty:
                return None

            def _latest(col_name_pattern: str) -> Optional[float]:
                col = next((c for c in df.columns if col_name_pattern in str(c)), None)
                if not col:
                    return None
                vals = df[col].dropna().head(4).tolist()
                return round(float(vals[0]), 2) if vals else None

            roe = _latest("净资产收益率")  # 取加权净资产收益率更准确
            if roe is None:
                roe = _latest("总资产净利润率")

            # 营收增长：主营业务收入增长率（最新一期）
            rev_growth = _latest("主营业务收入增长率")

            # 现金流：每股经营性现金流
            cash_flow = _latest("每股经营性现金流")

            # 也尝试总资产净利润率作为备选
            if roe is None:
                roe = _latest("总资产净利润率")

            return FinancialReport(
                roe=roe,
                revenue_growth=rev_growth,
                operating_cash_flow=cash_flow,
            )
        except Exception:
            return None


class AkShareFlowAdapter:
    """AkShare 资金流向 adapter。"""

    async def get_fund_flow(self, code: str, days: int = 5) -> Optional[FundFlow]:
        return await asyncio.to_thread(self._get_flow_sync, code, days)

    def _get_flow_sync(self, code: str, days: int) -> Optional[FundFlow]:
        try:
            import akshare as ak
            market = "sh" if code.startswith(("6", "9")) else "sz"
            df = ak.stock_individual_fund_flow(stock=code, market=market)
            if df is None or df.empty:
                return None

            recent = df.tail(days)
            total_net = 0
            outflow_streak = 0
            for _, row in recent.iterrows():
                main_net = 0
                for col in row.index:
                    if "主力" in str(col) and "净" in str(col):
                        main_net = float(row[col]) if pd.notna(row[col]) else 0
                        break
                total_net += main_net
                if main_net < -5_000_000:
                    outflow_streak += 1

            return FundFlow(
                net_inflow_1d=total_net,
                consecutive_outflow_days=outflow_streak,
            )
        except Exception:
            return None


# ---------------------------------------------------------------------------
# MX Adapters
# ---------------------------------------------------------------------------

class MXSentimentAdapter:
    """妙想舆情 adapter。"""

    async def search_news(self, query: str) -> Optional[SentimentData]:
        return await asyncio.to_thread(self._sync, query)

    def _sync(self, query: str) -> Optional[SentimentData]:
        try:
            from hermes.market.mx.search import MXSearch
            mx = MXSearch()
            result = mx.search(query)

            data = result.get("data", {})
            inner = data.get("data", {})
            search_resp = inner.get("llmSearchResponse", {})
            items = search_resp.get("data", [])

            if not items:
                return SentimentData(score=1.5, detail="无相关资讯")

            report_count = 0
            positive_count = 0
            negative_count = 0
            for item in items:
                info_type = item.get("informationType", "")
                rating = str(item.get("rating", "")).lower()
                if info_type == "REPORT":
                    report_count += 1
                if any(w in rating for w in ["买入", "增持", "推荐"]):
                    positive_count += 1
                elif any(w in rating for w in ["减持", "卖出"]):
                    negative_count += 1

            score = 1.5
            if report_count >= 5:
                score += 0.5
            elif report_count >= 2:
                score += 0.3
            if positive_count >= 2:
                score += 0.5
            elif positive_count >= 1:
                score += 0.3
            if negative_count >= 2:
                score -= 0.5
            score = max(0, min(score, 3.0))

            return SentimentData(
                score=round(score, 1),
                news_count=len(items),
                positive_ratio=positive_count / max(len(items), 1),
                detail=f"研报{report_count}篇 买入{positive_count} 减持{negative_count}",
            )
        except Exception:
            return None


class MXScreenerAdapter:
    """妙想选股 adapter。"""

    async def search_stocks(self, query: str) -> list[dict]:
        return await asyncio.to_thread(self._sync, query)

    def _sync(self, query: str) -> list[dict]:
        try:
            from hermes.market.mx.xuangu import MXXuangu
            mx = MXXuangu()
            result = mx.search(query)
            rows, _, err = mx.extract_data(result)
            if err:
                return []
            return rows
        except Exception:
            return []


class MXMarketAdapter:
    """妙想行情 adapter。"""

    async def get_realtime(self, codes: list[str]) -> dict[str, StockQuote]:
        return await asyncio.to_thread(self._get_realtime_sync, codes)

    async def get_kline(self, code: str, period: str = "daily", count: int = 120) -> Optional[pd.DataFrame]:
        return None

    async def get_index(self, symbols: list[str]) -> dict[str, IndexQuote]:
        return await asyncio.to_thread(self._get_index_sync)

    def _get_realtime_sync(self, codes: list[str]) -> dict[str, StockQuote]:
        try:
            from hermes.market.mx.realtime import get_realtime_mx
            raw = get_realtime_mx(codes)
            result = {}
            for code, data in raw.items():
                if "error" in data:
                    continue
                result[code] = StockQuote(
                    code=code,
                    name=data.get("name", code),
                    price=data.get("price", 0),
                    open=data.get("open", 0),
                    high=data.get("high", 0),
                    low=data.get("low", 0),
                    close=data.get("price", 0),
                    volume=int(data.get("volume", 0)),
                    amount=data.get("amount", 0),
                    change_pct=data.get("change_pct", 0),
                )
            return result
        except Exception:
            return {}

    def _get_index_sync(self) -> dict[str, IndexQuote]:
        """获取指数行情，优先 MX，失败则用 akshare 兜底。
        均线/above_ma20/below_ma60_days 由 akshare 日线数据计算。"""
        import akshare as ak
        import pandas as pd

        # 日线代码映射
        code_map = {
            "上证指数": "sh000001",
            "深证成指": "sz399001",
            "创业板指": "sz399006",
            "科创50": "sh000688",
        }

        def _compute_ma(symbol: str) -> tuple[float, float, bool, int]:
            """计算 MA20、MA60、above_ma20、below_ma60_days。"""
            try:
                df = ak.stock_zh_index_daily(symbol=symbol)
                df = df.sort_values("date")
                close = df["close"].astype(float)
                ma20_val = close.rolling(20).mean().iloc[-1] if len(close) >= 20 else 0
                ma60_val = close.rolling(60).mean().iloc[-1] if len(close) >= 60 else 0
                latest_price = close.iloc[-1]
                above = bool(latest_price > ma20_val > 0)
                # below_ma60_days：最近多少个交易日连续低于 MA60
                below_ma60 = (close < ma60_val).iloc[-20:] if ma60_val > 0 else pd.Series(False, index=close.index[-20:])
                count = 0
                for v in reversed(below_ma60.tolist()):
                    if v:
                        count += 1
                    else:
                        break
                return float(ma20_val), float(ma60_val), above, count
            except Exception:
                return 0.0, 0.0, False, 0

        result = {}

        # 优先 MX（获取实时价格/涨跌幅）
        try:
            from hermes.market.mx.realtime import get_market_index_mx
            raw = get_market_index_mx()
            for name, data in raw.items():
                if "error" in data:
                    continue
                symbol = code_map.get(name, name)
                ma20, ma60, above_ma20, below_days = _compute_ma(symbol)
                result[name] = IndexQuote(
                    symbol=symbol,
                    name=name,
                    price=data.get("close") or data.get("price", 0) or 0,
                    change_pct=data.get("change_pct", 0) or 0,
                    ma20=ma20,
                    ma60=ma60,
                    above_ma20=above_ma20,
                    below_ma60_days=below_days,
                )
            if result and any(v.price > 0 for v in result.values()):
                return result
        except Exception:
            pass

        # akshare 兜底（价格 + 均线）
        try:
            spot = ak.stock_zh_index_spot_sina()
            for name, code in code_map.items():
                if name in result:
                    continue
                row = spot[spot["代码"] == code]
                if row.empty:
                    continue
                r = row.iloc[0]
                ma20, ma60, above_ma20, below_days = _compute_ma(code)
                result[name] = IndexQuote(
                    symbol=code,
                    name=name,
                    price=float(r.get("最新价", 0) or 0),
                    change_pct=float(r.get("涨跌幅", 0) or 0),
                    ma20=ma20,
                    ma60=ma60,
                    above_ma20=above_ma20,
                    below_ma60_days=below_days,
                )
            return result
        except Exception:
            return {}
