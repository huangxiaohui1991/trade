"""
market/adapters.py — 数据源 Protocol + adapter 实现

Protocol 定义标准接口，adapter 实现具体数据源。
所有 adapter 返回标准化的 dataclass，不暴露数据源细节。
"""

from __future__ import annotations

import asyncio
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Protocol, runtime_checkable

import pandas as pd

_logger = logging.getLogger(__name__)

from hermes.platform.time import local_now, local_today
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
    """AkShare 行情 adapter（仅 A 股）。"""

    async def get_realtime(self, codes: list[str]) -> dict[str, StockQuote]:
        # 港股代码不处理（避免把 09927 当成 A 股 601127）
        cn_codes = [c for c in codes if not is_hk_code(c)]
        if not cn_codes:
            return {}
        return await asyncio.to_thread(self._get_realtime_sync, cn_codes)

    async def get_kline(self, code: str, period: str = "daily", count: int = 120) -> Optional[pd.DataFrame]:
        if is_hk_code(code):
            return None
        return await asyncio.to_thread(self._get_kline_sync, code, period, count)

    async def get_index(self, symbols: list[str]) -> dict[str, IndexQuote]:
        # AkShare 不负责指数行情，指数由 MXMarketAdapter 提供（见 _get_index_sync stub）
        return await asyncio.to_thread(self._get_index_sync, symbols)

    async def get_sector_heatmap(self) -> list[dict]:
        """获取行业板块热力图数据。

        优先东财（stock_board_industry_name_em），失败则降级新浪行业
        （stock_sector_spot），两者独立数据源。
        返回字段：板块名称、涨跌幅、成交额、上涨家数、下跌家数。
        按涨跌幅降序排列。
        """
        return await asyncio.to_thread(self._get_sector_heatmap_sync)

    def _get_sector_heatmap_sync(self) -> list[dict]:
        """同步实现：拉东财行业板块行情，失败则降级到新浪行业。"""
        # 方式1：东财行业板块
        df = self._fetch_em_industry()
        if df is not None:
            return df

        # 方式2：新浪行业（独立数据源）
        df2 = self._fetch_sina_industry()
        if df2 is not None:
            return df2

        _logger.warning("[AkShareMarket] 行业板块：东财和新浪均获取失败")
        return []

    def _fetch_em_industry(self) -> Optional[list[dict]]:
        """拉东财行业板块（stock_board_industry_name_em）。"""
        try:
            import akshare as ak
            df = ak.stock_board_industry_name_em()
            if df is None or df.empty:
                return None
            rename = {
                "板块名称": "name",
                "涨跌幅": "change_pct",
                "成交额": "amount",
                "上涨家数": "up_count",
                "下跌家数": "down_count",
            }
            col_map = {}
            for old, new in rename.items():
                for col in df.columns:
                    if old in col:
                        col_map[col] = new
                        break
            if not col_map:
                return None
            df = df.rename(columns=col_map)
            keep = ["name", "change_pct", "amount", "up_count", "down_count"]
            df = df[[c for c in keep if c in df.columns]]
            df = df.sort_values("change_pct", ascending=False).reset_index(drop=True)
            records = df.to_dict("records")
            for r in records:
                r["change_pct"] = float(r.get("change_pct", 0) or 0)
                r["amount"] = float(r.get("amount", 0) or 0)
                r["up_count"] = int(r.get("up_count", 0) or 0)
                r["down_count"] = int(r.get("down_count", 0) or 0)
            return records
        except Exception as e:
            _logger.warning(f"[AkShareMarket] 东财行业板块获取失败: {e}")
            return None

    def _fetch_sina_industry(self) -> Optional[list[dict]]:
        """拉新浪行业板块（stock_sector_spot），与东财独立。"""
        try:
            import akshare as ak
            df = ak.stock_sector_spot(indicator="新浪行业")
            if df is None or df.empty:
                return None
            # 新浪列名：板块、涨跌幅、总成交额、公司家数
            df = df.rename(columns={
                "板块": "name",
                "涨跌幅": "change_pct",
                "总成交额": "amount",
                "公司家数": "up_count",
            })
            keep = ["name", "change_pct", "amount", "up_count"]
            df = df[[c for c in keep if c in df.columns]]
            df = df.sort_values("change_pct", ascending=False).reset_index(drop=True)
            records = df.to_dict("records")
            for r in records:
                r["change_pct"] = float(r.get("change_pct", 0) or 0)
                r["amount"] = float(r.get("amount", 0) or 0)
                r["up_count"] = int(r.get("up_count", 0) or 0)
                r["down_count"] = 0  # 新浪无涨跌家数，置 0
            return records
        except Exception as e:
            _logger.warning(f"[AkShareMarket] 新浪行业板块获取失败: {e}")
            return None

    def _get_realtime_sync(self, codes: list[str]) -> dict[str, StockQuote]:
        """主入口：优先东财全量，失败则降级到新浪分时。"""
        try:
            import akshare as ak

            # 过滤掉港股代码
            a_codes = [c for c in codes if not is_hk_code(c)]
            if not a_codes:
                return {}

            df = ak.stock_zh_a_spot_em()
            if df is not None and not df.empty:
                code_set = set(a_codes)
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
                if result:
                    return result
        except Exception:
            pass

        # 东财断线，降级到新浪分时（收盘后场景，每只股票单独请求）
        return self._get_realtime_from_sina_tick(codes)

    def _get_realtime_from_sina_tick(self, codes: list[str]) -> dict[str, StockQuote]:
        """新浪分时降级：用当日最后一笔的价格和成交额估算实时行情。

        仅在东财 stock_zh_a_spot_em 断线时使用。
        收盘后可用，日内数据有限（无盘前集合竞价价格）。
        """
        from datetime import date

        result = {}
        today = local_today().strftime("%Y%m%d")

        for code in codes:
            if is_hk_code(code):
                continue
            try:
                import akshare as ak

                # 标准化新浪格式
                symbol = f"sh{code}" if code.startswith(("6", "9")) else f"sz{code}"
                df = ak.stock_intraday_sina(symbol=symbol, date=today)
                if df is None or df.empty:
                    continue

                last_row = df.iloc[-1]
                price = float(last_row["price"])
                prev_price = float(last_row["prev_price"]) if last_row["prev_price"] else price
                change_pct = ((price - prev_price) / prev_price * 100) if prev_price > 0 else 0.0

                # 成交额 = sum(price * volume)，新浪分时 volume 是累计成交量
                total_volume = float(df["volume"].sum()) if "volume" in df.columns else 0.0
                amount = float(last_row["volume"]) * price if "volume" in df.columns else 0.0

                result[code] = StockQuote(
                    code=code,
                    name=str(df.iloc[0]["name"]) if "name" in df.columns else code,
                    price=price,
                    open=0.0,  # 新浪分时无开盘价
                    high=float(df["price"].max()),
                    low=float(df["price"].min()),
                    close=price,
                    volume=int(total_volume),
                    amount=amount,
                    change_pct=round(change_pct, 2),
                )
            except Exception:
                continue

        return result

    def _get_kline_sync(self, code: str, period: str, count: int) -> Optional[pd.DataFrame]:
        """主入口：优先东财日线，失败则降级到腾讯日线。"""
        result = self._get_kline_from_em(code, count)
        if result is not None and not result.empty:
            return result
        return self._get_kline_from_tx(code, count)

    def _get_kline_from_em(self, code: str, count: int) -> Optional[pd.DataFrame]:
        """东财日线（复权）。"""
        try:
            import akshare as ak

            if code.startswith(("6", "9")):
                symbol = f"sh{code}"
            else:
                symbol = f"sz{code}"

            df = ak.stock_zh_a_daily(symbol=symbol, adjust="qfq")
            if df is None or df.empty:
                return None

            df = df.sort_values("date").tail(count * 2).reset_index(drop=True)
            df["涨跌幅"] = df["close"].pct_change() * 100
            return df
        except Exception:
            return None

    def _get_kline_from_tx(self, code: str, count: int) -> Optional[pd.DataFrame]:
        """腾讯日线降级：无成交量（volume=0），其他字段齐全。

        适用：东财 stock_zh_a_daily 断线时。
        """
        try:
            import akshare as ak
            from datetime import datetime, timedelta

            if code.startswith(("6", "9")):
                symbol = f"sh{code}"
            else:
                symbol = f"sz{code}"

            end = datetime.today().strftime("%Y%m%d")
            start = (datetime.today() - timedelta(days=count * 4)).strftime("%Y%m%d")
            df = ak.stock_zh_a_hist_tx(symbol=symbol, start_date=start, end_date=end)
            if df is None or df.empty:
                return None

            df = df.sort_values("date").tail(count * 2).reset_index(drop=True)

            # 腾讯日线无 volume，用 amount/close 估算成交量（单位一致时有效）
            # 保留 amount 列，volume 置 0（不影响技术指标计算，仅影响 amount 相关维度）
            if "volume" not in df.columns:
                df["volume"] = 0
            if "amount" not in df.columns:
                df["amount"] = 0.0

            df["涨跌幅"] = df["close"].pct_change() * 100
            return df
        except Exception:
            return None

    def _get_index_sync(self, symbols: list[str]) -> dict[str, IndexQuote]:
        # 指数行情未实现，由 MXMarketAdapter 提供，此处仅作兜底
        _logger.debug("[AkShareMarket] get_index 未实现，返回空")
        return {}


class AkShareFinancialAdapter:
    """AkShare 财务 adapter。"""

    async def get_financial(self, code: str) -> Optional[FinancialReport]:
        return await asyncio.to_thread(self._get_financial_sync, code)

    def _get_financial_sync(self, code: str) -> Optional[FinancialReport]:
        try:
            if is_hk_code(code):
                return None
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
        """主入口：优先东财，失败则降级到腾讯 tick。"""
        try:
            if is_hk_code(code):
                return None
            # 1. 优先东财资金流
            result = self._get_flow_from_em(code, days)
            if result is not None:
                return result
            # 2. 东财断线，降级到腾讯分笔
            result = self._get_flow_from_tx_tick(code)
            if result is not None:
                return result
            return None
        except Exception:
            return None

    def _get_flow_from_em(self, code: str, days: int) -> Optional[FundFlow]:
        """东财资金流接口，失败时返回 None（ caller 会尝试腾讯降级）。"""
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

    def _get_flow_from_tx_tick(self, code: str) -> Optional[FundFlow]:
        """腾讯分笔成交降级：按买盘/卖盘汇总估算主力净流入。

        适用场景：东财 stock_individual_fund_flow 接口断线时。
        注意：腾讯 tick 只有当日数据，days 参数被忽略。
        """
        try:
            import akshare as ak

            # 标准化为腾讯格式：sh600415 / sz000001
            if code.startswith(("6", "9")):
                symbol = f"sh{code}"
            else:
                symbol = f"sz{code}"

            df = ak.stock_zh_a_tick_tx_js(symbol=symbol)
            if df is None or df.empty:
                return None

            buy_mask = df["性质"].str.contains("买", na=False)
            sell_mask = df["性质"].str.contains("卖", na=False)

            buy_amount = df.loc[buy_mask, "成交金额"].sum()
            sell_amount = df.loc[sell_mask, "成交金额"].sum()
            total_amount = buy_amount + sell_amount

            net_inflow = buy_amount - sell_amount  # 正=净流入，负=净流出

            # 主买占比（主力参与度）
            main_force_ratio = buy_amount / total_amount if total_amount > 0 else 0.0

            # 连续流出判断：今日净流出且金额 > 5000万 → outflow_streak=1（腾讯tick只当日）
            # 明日需要重新获取才知道是否连续，所以最多记1天
            outflow_streak = 1 if (net_inflow < -5_000_000) else 0

            return FundFlow(
                net_inflow_1d=net_inflow,
                net_inflow_5d=net_inflow,  # 腾讯tick仅当日，退化为单日
                main_force_ratio=main_force_ratio,
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
        a_codes = [code for code in codes if not is_hk_code(code)]
        if not a_codes:
            return {}
        return await asyncio.to_thread(self._get_realtime_sync, a_codes)

    async def get_index(self, symbols: list[str]) -> dict[str, IndexQuote]:
        """获取 A 股指数行情（内部 hardcode 四大指数，无视 symbols 参数）。"""
        return await asyncio.to_thread(self._get_index_sync)

    async def get_kline(self, code: str, period: str = "daily", count: int = 120) -> Optional[pd.DataFrame]:
        if is_hk_code(code):
            return None
        return await asyncio.to_thread(self._get_kline_sync, code, period, count)

    def _get_kline_sync(self, code: str, period: str = "daily", count: int = 120) -> Optional[pd.DataFrame]:
        """从 MX finskillshub API 获取日K线历史数据。

        策略：query= "code 近N个交易日 日K线"，DTO 0 含：
          - headName[]    → 日期列（最新→最旧，降序）
          - 100000000017969 → 开盘价
          - 100000000017975 → 收盘价
          - 100000000019180 → 是否涨停（辅助）
        数据只有开/收，没有 high/low/vol，compute_technical_indicators
        用收盘价计算均线/R SI/MACD，只需 close 列存在即可（其他列填0）。

        兜底：AkShare 东财日线。
        """
        if is_hk_code(code):
            _logger.debug(f"[MXMarket] get_kline 跳过港股代码: {code}")
            return None

        if period != "daily":
            _logger.warning(f"[MXMarket] get_kline 不支持 period={period}，仅支持 daily")
            return None

        import akshare as ak

        # 先试 MX
        mx_df = self._get_kline_from_mx(code, count)
        if mx_df is not None and not mx_df.empty:
            return mx_df

        # MX 失败 → AkShare 东财
        try:
            symbol = f"sh{code}" if code.startswith(("6", "9")) else f"sz{code}"
            df = ak.stock_zh_a_daily(symbol=symbol, adjust="qfq")
            if df is None or df.empty:
                return None
            df = df.sort_values("date").tail(count * 2).reset_index(drop=True)
            df["涨跌幅"] = df["close"].pct_change() * 100
            return df
        except Exception:
            return None

    def _get_kline_from_mx(self, code: str, count: int) -> Optional[pd.DataFrame]:
        """调用 MX finskillshub query 接口拉日K线。"""
        try:
            import httpx
        except ImportError:
            return None

        # 加载 MX API key（与 realtime.py 相同逻辑）
        import os
        from pathlib import Path

        def _load_key():
            key = os.environ.get("MX_APIKEY", "").strip()
            if key:
                return key
            p = Path(__file__).resolve().parent.parent.parent.parent / ".env"
            if p.exists():
                for line in open(p, encoding="utf-8"):
                    line = line.strip()
                    if line.startswith("MX_APIKEY=") and len(line) > 10:
                        return line.split("=", 1)[1].strip()
            return ""

        api_key = _load_key()
        if not api_key:
            return None

        # MX finnhub 返回结构：
        # DTO 0: headName=日期[], 100000000017969=开盘[], 100000000017975=收盘[]
        # 日期列从新→旧排列，需要反转
        _FID_OPEN = "100000000017969"
        _FID_CLOSE = "100000000017975"

        try:
            resp = httpx.post(
                "https://mkapi2.dfcfs.com/finskillshub/api/claw/query",
                json={"toolQuery": f"{code} 近{count}个交易日 日K线"},
                headers={"Content-Type": "application/json", "apikey": api_key},
                timeout=15.0,
            )
            data = resp.json()
        except Exception:
            return None

        try:
            inner = data.get("data", {}).get("data", {})
            dto_list = inner.get("searchDataResultDTO", {}).get("dataTableDTOList", [])
        except Exception:
            return None

        dto = None
        for d in dto_list:
            raw = d.get("rawTable", {})
            if _FID_OPEN in raw and _FID_CLOSE in raw:
                dto = d
                break
        if dto is None:
            return None

        raw = dto.get("rawTable", {})
        head = raw.get("headName", [])

        if not isinstance(head, list) or len(head) < 2:
            return None

        dates = list(reversed(head))            # 新→旧 → 旧→新
        opens = list(reversed(raw.get(_FID_OPEN, [])))
        closes = list(reversed(raw.get(_FID_CLOSE, [])))

        rows = min(len(dates), count * 2)
        rows = max(rows, 5)

        import pandas as pd
        df = pd.DataFrame({
            "date":   dates[-rows:],
            "open":   [float(o) for o in opens[-rows:]],
            "close":  [float(c) for c in closes[-rows:]],
            # MX 没有这些字段，akshare adapter 的 compute_technical_indicators
            # 只强制要求 close，其余列填 0 或 NaN
            "high":   [float(c) for c in closes[-rows:]],   # 近似
            "low":    [float(c) for c in closes[-rows:]],   # 近似
            "volume": [0] * rows,
        })
        df["涨跌幅"] = df["close"].pct_change() * 100
        return df

    def _get_realtime_sync(self, codes: list[str]) -> dict[str, StockQuote]:
        try:
            codes = [code for code in codes if not is_hk_code(code)]
            if not codes:
                return {}
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
                # MX 可能只返回部分指数，缺失的由 akshare 兜底补全
                missing = [n for n in code_map if n not in result]
                if not missing:
                    return result
                # 有缺失，继续走 akshare 兜底（只补缺失的）
        except Exception as e:
            _logger.warning(f"[MXMarket] MX 指数行情获取失败: {e}")

        # akshare 兜底（价格 + 均线）
        # 注意：stock_zh_index_spot_sina 在批量请求时对部分指数（深证/创业板）返回 0，
        # 改用 stock_zh_index_daily 的最新收盘价作为价格来源，更稳定
        try:
            # Phase 1: 并行拉取所有指数日线数据
            def _fetch_daily(code):
                try:
                    return code, ak.stock_zh_index_daily(symbol=code)
                except Exception:
                    return code, None

            codes = list(code_map.values())
            with ThreadPoolExecutor(max_workers=min(len(codes), 4)) as executor:
                daily_map: dict[str, Optional[pd.DataFrame]] = {
                    code: df for code, df in executor.map(_fetch_daily, codes)
                }

            # Phase 2: 主线程更新 result（已有 MX 数据的补充价格/MA，没有的创建新记录）
            for name, code in code_map.items():
                daily_df = daily_map.get(code)
                if name in result:
                    # MX 已返回数据，用日线补充/替换价格（如果 MX 价格无效）和 MA
                    # 注意：IndexQuote 是 frozen dataclass，不能原地修改，需重建对象
                    existing = result[name]
                    price = existing.price
                    change_pct = existing.change_pct
                    ma20 = existing.ma20
                    ma60 = existing.ma60
                    above_ma20 = existing.above_ma20
                    below_ma60_days = existing.below_ma60_days
                    if daily_df is not None and not daily_df.empty:
                        latest_close = float(daily_df["close"].iloc[-1])
                        prev_close = float(daily_df["close"].iloc[-2]) if len(daily_df) >= 2 else latest_close
                        if (price or 0) <= 0 or price != latest_close:
                            price = latest_close
                        daily_chg = ((latest_close - prev_close) / prev_close * 100) if prev_close > 0 else 0
                        if abs(change_pct) < 0.01 and abs(daily_chg) > 0.01:
                            change_pct = daily_chg
                    # 补全 MA（如果 MX 没有的话）
                    if not ma20 and not ma60:
                        ma20, ma60, above_ma20, below_days = _compute_ma(code)
                        below_ma60_days = below_days
                    # 重建 IndexQuote（替换整个对象）
                    result[name] = IndexQuote(
                        symbol=existing.symbol,
                        name=name,
                        price=price,
                        change_pct=change_pct,
                        ma20=ma20,
                        ma60=ma60,
                        above_ma20=above_ma20,
                        below_ma60_days=below_ma60_days,
                        timestamp=existing.timestamp,
                    )
                else:
                    # 完全没有数据，用日线构建完整记录
                    price_val = 0.0
                    change_val = 0.0
                    if daily_df is not None and not daily_df.empty:
                        latest_close = float(daily_df["close"].iloc[-1])
                        prev_close = float(daily_df["close"].iloc[-2]) if len(daily_df) >= 2 else latest_close
                        price_val = latest_close
                        change_val = ((latest_close - prev_close) / prev_close * 100) if prev_close > 0 else 0.0
                    ma20, ma60, above_ma20, below_days = _compute_ma(code)
                    result[name] = IndexQuote(
                        symbol=code,
                        name=name,
                        price=price_val,
                        change_pct=change_val,
                        ma20=ma20,
                        ma60=ma60,
                        above_ma20=above_ma20,
                        below_ma60_days=below_days,
                    )
            return result
        except Exception:
            return {}


# ---------------------------------------------------------------------------
# BaoStock Adapters
# ---------------------------------------------------------------------------

# 模块级 session 管理，避免频繁 login/logout
_bs_lock = threading.RLock()
_bs_logged_in = False


def _bs_ensure_login():
    """确保已登录 baostock（线程安全）。"""
    global _bs_logged_in
    with _bs_lock:
        if not _bs_logged_in:
            import baostock as bs
            lg = bs.login()
            if lg.error_code != "0":
                raise RuntimeError(f"baostock login failed: {lg.error_msg} {lg.error_msg}")
            _bs_logged_in = True


def _bs_logout():
    """登出 baostock。"""
    global _bs_logged_in
    with _bs_lock:
        if _bs_logged_in:
            import baostock as bs
            bs.logout()
            _bs_logged_in = False


def _normalize_baostock_code(code: str) -> str:
    """将纯数字 A 股代码标准化为 baostock 格式（sh.600000 / sz.000001）。

    baostock 代码规则：
    - 沪市：sh.6xxxxx
    - 深市：sz.0xxxxx / sz.3xxxxx（创业板）
    - 北交所：bj.8xxxxx
    """
    code = code.strip()
    if "." in code:
        return code.lower()
    if code.startswith(("6", "9")):
        return f"sh.{code}"
    if code.startswith(("0", "3")):
        return f"sz.{code}"
    if code.startswith("8"):
        return f"bj.{code}"
    # 兜底：加 sh
    return f"sh.{code}"


def _to_baostock_code(code: str) -> str:
    """兼容处理：code 可能已经是 sh.600000 格式或纯数字。"""
    return _normalize_baostock_code(code)


class BaoStockMarketAdapter:
    """BaoStock 历史数据 adapter（主要用于回测场景）。

    支持：
    - 日/周/月 K 线（前复权、后复权、不复权）
    - 5/15/30/60 分钟 K 线
    - 指数 K 线

    注意：baostock 不提供实时行情，get_realtime() 返回最新一日快照
    （等同于当日收盘价），仅作兜底使用。
    """

    async def get_realtime(self, codes: list[str]) -> dict[str, StockQuote]:
        # baostock 无真正实时，此处用最新日K 快照兜底
        result = {}
        for code in codes:
            if is_hk_code(code):
                continue
            bs_code = _to_baostock_code(code)
            df = await self.get_kline(bs_code, period="daily", count=1)
            if df is not None and not df.empty:
                row = df.iloc[-1]
                result[code] = StockQuote(
                    code=code,
                    name=str(row.get("名称", code)),
                    price=float(row.get("收盘", 0)),
                    open=float(row.get("开盘", 0)),
                    high=float(row.get("最高", 0)),
                    low=float(row.get("最低", 0)),
                    close=float(row.get("收盘", 0)),
                    volume=int(float(row.get("成交量", 0))),
                    amount=float(row.get("成交额", 0)),
                    change_pct=float(row.get("涨跌幅", 0) or 0),
                )
        return result

    async def get_kline(
        self,
        code: str,
        period: str = "daily",
        count: int = 120,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        adjustflag: str = "2",
    ) -> Optional[pd.DataFrame]:
        """获取 K 线数据。

        Args:
            code: 股票代码（支持 sh.600000 / sz.000001 / 600000 等格式）
            period: 日线周期
                - "daily" / "d": 日 K（默认）
                - "weekly" / "w": 周 K
                - "monthly" / "m": 月 K
                - "5"/"15"/"30"/"60": 分钟 K
            count: 获取最近 N 条（与 start_date 二选一）
            start_date: 开始日期 "YYYY-MM-DD"（与 count 二选一）
            end_date: 结束日期 "YYYY-MM-DD"
            adjustflag: 复权类型
                - "3": 不复权（默认）
                - "2": 前复权（回测推荐）
                - "1": 后复权

        Returns:
            DataFrame，列名对齐 MarketStore.save_bars()：
            日期, 开盘, 最高, 最低, 收盘, 成交量, 成交额, 涨跌幅, 证券名称
        """
        return await asyncio.to_thread(
            self._get_kline_sync, code, period, count, start_date, end_date, adjustflag
        )

    def _get_kline_sync(
        self,
        code: str,
        period: str,
        count: int,
        start_date: Optional[str],
        end_date: Optional[str],
        adjustflag: str,
    ) -> Optional[pd.DataFrame]:
        try:
            import baostock as bs

            bs_code = _to_baostock_code(code)
            _bs_ensure_login()

            # period 标准化
            freq_map = {"daily": "d", "d": "d", "weekly": "w", "w": "w", "monthly": "m", "m": "m",
                        "5": "5", "15": "15", "30": "30", "60": "60"}
            freq = freq_map.get(period, "d")

            # 字段列表
            # 注意：分钟线不支持 pctChg 和 turn
            is_minute = freq not in ("d", "w", "m")
            fields = "date,code,open,high,low,close,volume,amount,adjustflag"
            if not is_minute:
                fields += ",pctChg"
            if freq in ("w", "m"):
                fields += ",turn"
            if is_minute:
                fields += ",time"

            # 计算日期范围（当指定 count 而非起止日期时）
            if start_date is None and count > 0:
                from datetime import datetime, timedelta
                end_dt = local_now().replace(tzinfo=None)
                if freq == "d":
                    days = min(count * 3, 5000)
                elif freq in ("w", "m"):
                    # 周/月线：每单元跨度大
                    weeks_per_bar = 1 if freq == "w" else 4
                    days = min(count * weeks_per_bar * 14, 5000)
                else:
                    # 分钟线：每交易日约 240 条，估算所需交易日
                    trading_days = (count // 240) + 2
                    days = trading_days * 7 + 7
                start_dt = end_dt - timedelta(days=days)
                start_date = start_dt.strftime("%Y-%m-%d")
                end_date = end_dt.strftime("%Y-%m-%d")

            rs = bs.query_history_k_data_plus(
                bs_code,
                fields,
                start_date=start_date or "",
                end_date=end_date or "",
                frequency=freq,
                adjustflag=adjustflag,
            )

            if rs.error_code != "0":
                return None

            data_list = []
            while rs.error_code == "0" and rs.next():
                data_list.append(rs.get_row_data())

            if not data_list:
                return None

            df = pd.DataFrame(data_list, columns=rs.fields)

            # 取最近 count 条
            if count > 0 and len(df) > count:
                df = df.tail(count).reset_index(drop=True)

            # 列名对齐 MarketStore.save_bars()
            rename = {
                "date": "日期",
                "open": "开盘",
                "high": "最高",
                "low": "最低",
                "close": "收盘",
                "volume": "成交量",
                "amount": "成交额",
                "pctChg": "涨跌幅",
                "adjustflag": "复权类型",
                "code": "证券代码",
                "turn": "换手率",
            }
            df.rename(columns=rename, inplace=True)

            # 分钟数据：组合 date + time → 日期（baostock 分钟 time=YYYYMMDDHHMMSSmmm）
            if is_minute and "time" in df.columns:
                def _combine_dt(row):
                    t = str(row.get("time", ""))
                    if len(t) >= 14:
                        return f"{t[0:4]}-{t[4:6]}-{t[6:8]} {t[8:10]}:{t[10:12]}:{t[12:14]}"
                    return str(row.get("日期", ""))
                df["日期"] = df.apply(_combine_dt, axis=1)
                df.drop(columns=["time"], inplace=True)

            # 数值类型转换（baostock 全返回字符串）
            numeric_cols = ["开盘", "最高", "最低", "收盘", "成交量", "成交额", "涨跌幅", "换手率"]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

            # 获取证券名称
            name = self._get_stock_name(bs_code)
            df["证券名称"] = name
            df["名称"] = name

            return df

        except Exception:
            return None

    def _get_stock_name(self, bs_code: str) -> str:
        """通过 query_trade_dates 获取证券简称（近似）。"""
        try:
            import baostock as bs

            _bs_ensure_login()
            rs = bs.query_stock_basic(code=bs_code)
            if rs.error_code == "0":
                while rs.next():
                    row = rs.get_row_data()
                    if len(row) >= 3:
                        return row[3] or row[1] or bs_code
            return bs_code
        except Exception:
            return bs_code

    def __del__(self):
        """全局 session 不在此关闭，由模块级函数统一管理。"""
        pass


# ---------------------------------------------------------------------------
# 港股代码识别
# ---------------------------------------------------------------------------

def is_hk_code(code: str) -> bool:
    """判断是否为港股代码。

    港股代码规则：
    - 5 位纯数字且以 0 开头（如 09927, 00700, 01810）
    - 或显式带 hk 前缀（如 hk09927）
    """
    code = code.strip().lower()
    if code.startswith("hk"):
        return True
    # 5 位数字且以 0 开头 → 港股
    if len(code) == 5 and code.isdigit() and code.startswith("0"):
        # 排除 A 股深市 00xxx 开头的（深市主板 000xxx 是 6 位）
        # 5 位且 0 开头 → 港股
        return True
    return False


def normalize_hk_code(code: str) -> str:
    """标准化港股代码为 5 位纯数字（去掉 hk 前缀）。"""
    code = code.strip().lower()
    if code.startswith("hk"):
        code = code[2:]
    return code.zfill(5)


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
