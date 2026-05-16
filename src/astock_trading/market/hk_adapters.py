"""Hong Kong stock adapter helpers and AkShare adapters."""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

import pandas as pd

from astock_trading.market.models import FinancialReport, IndexQuote, StockQuote

from .adapter_utils import is_hk_code, normalize_hk_code

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# AkShare 港股 Adapters
# ---------------------------------------------------------------------------

class AkShareHKMarketAdapter:
    """AkShare 港股行情 adapter。

    数据源：
    - 实时行情：stock_hk_spot_em()（东财港股全市场快照）
    - K 线：stock_hk_daily(symbol, adjust='qfq')
    """

    async def get_realtime(self, codes: list[str]) -> dict[str, StockQuote]:
        hk_codes = [c for c in codes if is_hk_code(c)]
        if not hk_codes:
            return {}
        return await asyncio.to_thread(self._get_realtime_sync, hk_codes)

    async def get_kline(self, code: str, period: str = "daily", count: int = 120) -> Optional[pd.DataFrame]:
        if not is_hk_code(code):
            return None
        return await asyncio.to_thread(self._get_kline_sync, code, period, count)

    async def get_index(self, symbols: list[str]) -> dict[str, IndexQuote]:
        _logger.debug("[AkShareHKMarket] 港股指数暂不支持")
        return {}  # 港股指数暂不支持

    def _get_realtime_sync(self, codes: list[str]) -> dict[str, StockQuote]:
        try:
            import akshare as ak
            df = ak.stock_hk_spot_em()
            if df is None or df.empty:
                return {}

            # 标准化待查代码
            lookup = {}
            for c in codes:
                norm = normalize_hk_code(c)
                lookup[norm] = c  # norm → original code

            result = {}
            for _, row in df.iterrows():
                raw_code = str(row.get("代码", "")).strip()
                # stock_hk_spot_em 的代码列可能是 "09927" 或 "9927"
                norm = raw_code.zfill(5)
                if norm not in lookup:
                    continue
                original = lookup[norm]
                result[original] = StockQuote(
                    code=original,
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

            symbol = normalize_hk_code(code)
            df = ak.stock_hk_daily(symbol=symbol, adjust="qfq")
            if df is None or df.empty:
                return None

            # stock_hk_daily 返回英文列名：date, open, high, low, close, volume, amount
            df = df.sort_values("date").tail(count * 2).reset_index(drop=True)

            # 添加涨跌幅列
            df["close"] = pd.to_numeric(df["close"], errors="coerce")
            df["涨跌幅"] = df["close"].pct_change() * 100

            return df
        except Exception:
            return None


class AkShareHKFinancialAdapter:
    """港股财务 adapter — 使用 akshare 港股财务接口。

    akshare 港股财务数据有限，尽力获取，获取不到返回 None。
    """

    async def get_financial(self, code: str) -> Optional[FinancialReport]:
        if not is_hk_code(code):
            return None
        return await asyncio.to_thread(self._get_financial_sync, code)

    def _get_financial_sync(self, code: str) -> Optional[FinancialReport]:
        try:
            import akshare as ak
            symbol = normalize_hk_code(code)

            # 尝试 stock_hk_valuation_baidu（百度港股估值）
            try:
                df = ak.stock_hk_valuation_baidu(symbol=symbol, indicator="总市值", period="近一年")
                if df is not None and not df.empty:
                    # 只能拿到估值，没有 ROE 等
                    return FinancialReport()
            except Exception as e:
                _logger.debug(f"[AkShareHKFinancial] {code} 百度估值接口失败: {e}")

            # 港股财务数据有限，返回空报告（不阻塞评分，降级处理）
            _logger.info(f"[AkShareHKFinancial] {code} 港股财务数据有限，降级返回空报告")
            return FinancialReport()
        except Exception as e:
            _logger.warning(f"[AkShareHKFinancial] {code} 财务数据获取异常: {e}")
            return None
