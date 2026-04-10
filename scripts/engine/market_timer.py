#!/usr/bin/env python3
"""
engine/market_timer.py — 大盘择时引擎

职责：
  - 判断大盘趋势：GREEN / YELLOW / RED / CLEAR
  - GREEN：多头，可正常买入
  - YELLOW：震荡，谨慎买入，减半
  - RED：清仓信号，不买入
  - CLEAR：MA60下方15日+，清仓+不抄底

算法（与 strategy.yaml 对齐）：
  GREEN = 连续 N 日站上 MA20（N = green_days，默认3）
  RED = 连续 N 日跌破 MA20（N = red_days，默认5）
  CLEAR = MA60下方连续 N 日（N = clear_days_ma60，默认15）

用法：
  from scripts.engine.market_timer import MarketTimer
  timer = MarketTimer()
  signal = timer.get_signal()  # "GREEN" | "YELLOW" | "RED" | "CLEAR"
  detail = timer.get_detail()   # dict with per-index data
"""

import os
import sys
import warnings
from datetime import datetime
from typing import Literal

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

os.environ["TQDM_DISABLE"] = "1"
warnings.filterwarnings("ignore")

import pandas as pd
import akshare as ak

from scripts.utils.cache import load_json_cache, save_json_cache
from scripts.utils.config_loader import get_strategy
from scripts.utils.logger import get_logger

_logger = get_logger("market_timer")


# ---------------------------------------------------------------------------
# 大盘指数配置
# ---------------------------------------------------------------------------
_INDICES = {
    "上证指数": {"symbol": "sh000001", "market_code": "000001"},
    "深证成指": {"symbol": "sz399001", "market_code": "399001"},
    "创业板指": {"symbol": "sz399006", "market_code": "399006"},
    "科创50": {"symbol": "sh000688", "market_code": "000688"},
}

_INDEX_NAME_BY_SYMBOL = {cfg["symbol"]: name for name, cfg in _INDICES.items()}


# ---------------------------------------------------------------------------
# MarketTimer
# ---------------------------------------------------------------------------

class MarketTimer:
    """
    大盘择时判断

    Attributes:
        signal: 当前信号（GREEN / YELLOW / RED / CLEAR）
        detail: 各指数详细数据
    """

    def __init__(self):
        self.strategy = get_strategy()
        self.market_cfg = self.strategy.get("market_timer", {})
        self.green_days = self.market_cfg.get("green_days", 3)
        self.red_days = self.market_cfg.get("red_days", 5)
        self.clear_days = self.market_cfg.get("clear_days_ma60", 15)

        self._signal: Literal["GREEN", "YELLOW", "RED", "CLEAR"] = "CLEAR"
        self._detail: dict = {}
        self._snapshot: dict = {}
        self._computed = False

    # ---------------------------------------------------------------------------
    # 公开接口
    # ---------------------------------------------------------------------------

    def get_signal(self) -> Literal["GREEN", "YELLOW", "RED", "CLEAR"]:
        """返回大盘信号"""
        self._ensure_computed()
        return self._signal

    def get_detail(self) -> dict:
        """返回详细数据（复用已计算的快照）"""
        self._ensure_computed()
        return self._detail

    def get_snapshot(self) -> dict:
        """返回统一的大盘快照"""
        self._ensure_computed()
        return dict(self._snapshot)

    def get_position_multiplier(self) -> float:
        """
        根据信号返回仓位系数
        GREEN → 1.0（正常仓位）
        YELLOW → 0.5（半仓）
        RED → 0.0（清仓）
        CLEAR → 0.0（不抄底）
        """
        signal = self.get_signal()
        return {"GREEN": 1.0, "YELLOW": 0.5, "RED": 0.0, "CLEAR": 0.0}.get(signal, 0.0)

    # ---------------------------------------------------------------------------
    # 核心计算
    # ---------------------------------------------------------------------------

    def _ensure_computed(self) -> None:
        if self._computed:
            return
        self._compute()

    def _compute(self) -> None:
        """计算大盘信号"""
        index_data = {}
        for name, cfg in _INDICES.items():
            symbol = cfg["symbol"]
            try:
                data = self._fetch_index_data(symbol)
                index_data[name] = data
            except Exception as e:
                _logger.warning(f"[{name}] 数据获取失败: {e}")
                index_data[name] = {"error": str(e)}

        self._detail = index_data

        # 综合信号判断
        green_count = 0
        red_count = 0
        clear_count = 0
        total = 0

        for name, data in index_data.items():
            if "error" in data:
                continue
            total += 1

            above_ma20 = data.get("above_ma20", False)
            below_ma60_days = data.get("below_ma60_days", 0)

            if above_ma20:
                green_count += 1
            else:
                red_count += 1

            if below_ma60_days >= self.clear_days:
                clear_count += 1

        if total == 0:
            self._signal = "CLEAR"
            self._snapshot = self._build_snapshot(index_data, self._signal)
            self._computed = True
            return

        green_pct = green_count / total
        clear_pct = clear_count / total

        # 优先级：CLEAR > RED > YELLOW > GREEN
        if clear_pct >= 0.6:
            self._signal = "CLEAR"
        elif green_pct >= 0.6:
            self._signal = "GREEN"
        elif green_pct >= 0.3:
            self._signal = "YELLOW"
        else:
            self._signal = "RED"
        self._snapshot = self._build_snapshot(index_data, self._signal)
        self._computed = True

    @staticmethod
    def _index_view(name: str, data: dict) -> dict:
        view = {
            "name": name,
            "symbol": data.get("symbol", ""),
            "market_code": data.get("market_code", ""),
            "as_of_date": data.get("as_of_date", ""),
            "close": data.get("close", 0),
            "ma20": data.get("ma20"),
            "ma60": data.get("ma60"),
            "ma20_pct": data.get("ma20_pct", 0),
            "ma60_pct": data.get("ma60_pct", 0),
            "above_ma20": data.get("above_ma20", False),
            "above_ma60_days": data.get("above_ma60_days", 0),
            "below_ma60_days": data.get("below_ma60_days", 0),
            "green_streak": data.get("green_streak", 0),
            "red_streak": data.get("red_streak", 0),
            "change_pct": data.get("change_pct", 0),
            "signal": data.get("signal", ""),
            "source": data.get("source", ""),
            "source_chain": list(data.get("source_chain", [])),
            "stale": data.get("stale", False),
        }
        if data.get("error"):
            view["error"] = data["error"]
        return view

    def _build_snapshot(self, index_data: dict, signal: str) -> dict:
        indices = {}
        sources = []
        source_chain = []
        as_of_dates = []

        for name, data in index_data.items():
            if "error" in data:
                indices[name] = {"name": name, "error": data["error"]}
                continue
            view = self._index_view(name, data)
            indices[name] = view
            if view.get("source"):
                sources.append(view["source"])
            source_chain.extend(view.get("source_chain", []))
            if view.get("as_of_date"):
                as_of_dates.append(view["as_of_date"])

        unique_sources = [item for item in dict.fromkeys(sources) if item]
        top_source = unique_sources[0] if len(unique_sources) == 1 else ("mixed" if unique_sources else "unknown")
        top_source_chain = [item for item in dict.fromkeys(source_chain) if item]
        top_as_of_date = sorted(as_of_dates)[-1] if as_of_dates else datetime.now().strftime("%Y-%m-%d")

        market = {}
        for name, data in indices.items():
            if "error" in data:
                continue
            market[name] = {
                "price": data.get("close", 0),
                "chg_pct": data.get("change_pct", 0),
                "ma20_pct": data.get("ma20_pct", 0),
                "ma60_pct": data.get("ma60_pct", 0),
                "ma60_days": data.get("below_ma60_days", 0),
                "signal": data.get("signal", ""),
            }

        return {
            "as_of_date": top_as_of_date,
            "signal": signal,
            "market_signal": signal,
            "source": top_source,
            "source_chain": top_source_chain,
            "indices": indices,
            "market": market,
        }

    def _normalize_cached_payload(self, name: str, symbol: str, market_code: str, payload: dict) -> dict:
        normalized = dict(payload)
        normalized.setdefault("name", name)
        normalized.setdefault("symbol", symbol)
        normalized.setdefault("market_code", market_code)
        normalized.setdefault("as_of_date", datetime.now().strftime("%Y-%m-%d"))
        normalized["stale"] = True
        normalized["source"] = normalized.get("source", "cache_market_timer")

        source_chain = list(normalized.get("source_chain", []))
        if "cache_market_timer" not in source_chain:
            source_chain.append("cache_market_timer")
        normalized["source_chain"] = [item for item in dict.fromkeys(source_chain) if item]

        if not normalized.get("signal"):
            close = normalized.get("close", 0)
            ma20 = normalized.get("ma20")
            below_ma60_days = normalized.get("below_ma60_days", 0)
            if below_ma60_days >= self.clear_days:
                normalized["signal"] = "CLEAR"
            elif ma20:
                normalized["signal"] = "GREEN" if close >= ma20 else "RED"
            else:
                normalized["signal"] = "RED"

        return normalized

    # ------------------------------------------------------------------
    # 实时行情（腾讯接口，作为涨跌副补充）
    # ------------------------------------------------------------------

    @staticmethod
    def _fetch_realtime(symbol: str) -> dict | None:
        """
        通过腾讯行情接口获取实时价格和涨跌幅。
        symbol: sh000001 / sz399001 等
        Returns: {"price": float, "chg_pct": float} or None
        """
        try:
            import urllib.request
            url = f"https://qt.gtimg.cn/q={symbol}"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://finance.qq.com"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                raw = resp.read().decode("gbk", errors="replace")
            fields = raw.strip().split("~")
            if len(fields) < 33:
                return None
            price_str = fields[3]
            pct_str = fields[32]
            if not price_str or not pct_str:
                return None
            return {
                "price": float(price_str),
                "chg_pct": float(pct_str),
            }
        except Exception:
            return None

    # ------------------------------------------------------------------
    # 历史数据（MX → akshare） + 实时补充
    # ------------------------------------------------------------------

    def _fetch_index_data(self, symbol: str) -> dict:
        """
        获取单个指数的技术数据（MX优先 → akshare fallback）
        盘中/收盘时段补充实时涨跌幅（腾讯接口）
        """
        cache_key = symbol.replace("/", "_")
        index_name = _INDEX_NAME_BY_SYMBOL.get(symbol, symbol)
        market_code = _INDICES.get(index_name, {}).get("market_code", "")
        # MX 优先：查指数历史数据
        try:
            from scripts.mx.mx_data import MXData
            mx = MXData()
            result = mx.query(f"{index_name} 近80个交易日收盘价")
            data = result.get("data", {}).get("data", {}).get("searchDataResultDTO", {})
            dto_list = data.get("dataTableDTOList", [])

            for dto in dto_list:
                table = dto.get("table", {})
                heads = table.get("headName", [])
                if not isinstance(heads, list) or len(heads) < 20:
                    continue

                data_keys = [k for k in table.keys() if k != "headName"]
                if not data_keys:
                    continue

                # 提取收盘价序列
                closes = []
                for idx, date_str in enumerate(heads):
                    for k in data_keys:
                        vals = table[k]
                        if idx < len(vals):
                            v_str = str(vals[idx]).replace("元", "").replace(",", "").strip()
                            try:
                                closes.append(float(v_str))
                            except (ValueError, TypeError):
                                closes.append(0)
                            break

                if len(closes) < 20:
                    continue

                # MX 返回倒序，翻转为正序
                closes = closes[::-1]
                import numpy as np
                arr = pd.Series(closes)
                ma20 = float(arr.rolling(20).mean().iloc[-1]) if len(arr) >= 20 else None
                ma60 = float(arr.rolling(60).mean().iloc[-1]) if len(arr) >= 60 else None
                close = closes[-1]
                ma20_pct = ((close / ma20) - 1) * 100 if ma20 else 0
                ma60_pct = ((close / ma60) - 1) * 100 if ma60 else 0
                change_pct = ((close / closes[-2]) - 1) * 100 if len(closes) > 1 and closes[-2] else 0

                # 连续站上/跌破 MA20
                green_streak = 0
                red_streak = 0
                ma20_series = arr.rolling(20).mean()
                for i in range(len(arr) - 1, max(19, len(arr) - self.red_days - 1) - 1, -1):
                    if pd.notna(ma20_series.iloc[i]):
                        if arr.iloc[i] >= ma20_series.iloc[i]:
                            green_streak += 1
                        else:
                            red_streak += 1
                        if green_streak + red_streak >= self.red_days:
                            break

                # MA60 下方天数
                below_ma60_days = 0
                above_ma60_days = 0
                ma60_series = arr.rolling(60).mean()
                for i in range(len(arr) - 1, max(59, len(arr) - self.clear_days - 1) - 1, -1):
                    if pd.notna(ma60_series.iloc[i]):
                        if arr.iloc[i] >= ma60_series.iloc[i]:
                            above_ma60_days += 1
                        else:
                            below_ma60_days += 1
                        if above_ma60_days + below_ma60_days >= self.clear_days:
                            break

                _logger.info(f"[market_timer] MX 成功 {index_name}: close={close} ma20={ma20}")
                payload = {
                    "name": index_name,
                    "symbol": symbol,
                    "market_code": market_code,
                    "as_of_date": datetime.now().strftime("%Y-%m-%d"),
                    "close": close,
                    "ma20": round(ma20, 2) if ma20 else None,
                    "ma60": round(ma60, 2) if ma60 else None,
                    "ma20_pct": ma20_pct,
                    "ma60_pct": ma60_pct,
                    "above_ma20": close >= ma20 if ma20 else False,
                    "above_ma60_days": above_ma60_days,
                    "below_ma60_days": below_ma60_days,
                    "green_streak": green_streak,
                    "red_streak": red_streak,
                    "signal": "CLEAR" if below_ma60_days >= self.clear_days else ("GREEN" if close >= ma20 else "RED"),
                    "change_pct": change_pct,
                    "source": "mx_data",
                    "source_chain": ["mx_data"],
                    "stale": False,
                }
                save_json_cache("market_timer", cache_key, payload, meta={"source": "mx_data"})
                # 补充实时涨跌幅
                rt = self._fetch_realtime(symbol)
                if rt:
                    payload["close"] = rt["price"]
                    payload["change_pct"] = rt["chg_pct"]
                return payload
        except Exception as e:
            _logger.info(f"[market_timer] MX 失败 {symbol}: {e}")

        # akshare fallback
        try:
            df = ak.stock_zh_index_daily(symbol=symbol)
        except Exception as e:
            cached = load_json_cache("market_timer", cache_key, max_age_seconds=86400)
            if cached and isinstance(cached.get("data"), dict):
                payload = self._normalize_cached_payload(index_name, symbol, market_code, cached["data"])
                payload["cached_at"] = cached.get("cached_at")
                return payload
            raise ValueError(f"无法获取 {symbol} 数据: {e}")
        if df is None or df.empty:
            cached = load_json_cache("market_timer", cache_key, max_age_seconds=86400)
            if cached and isinstance(cached.get("data"), dict):
                payload = self._normalize_cached_payload(index_name, symbol, market_code, cached["data"])
                payload["cached_at"] = cached.get("cached_at")
                return payload
            raise ValueError(f"无法获取 {symbol} 数据")

        df = df.sort_values("date").tail(80).copy()
        df["MA20"] = df["close"].rolling(20).mean()
        df["MA60"] = df["close"].rolling(60).mean()

        latest = df.iloc[-1]
        close = float(latest["close"])
        ma20 = float(latest["MA20"]) if pd.notna(latest["MA20"]) else None
        ma60 = float(latest["MA60"]) if pd.notna(latest["MA60"]) else None
        ma20_pct = ((close / ma20) - 1) * 100 if ma20 else 0
        ma60_pct = ((close / ma60) - 1) * 100 if ma60 else 0

        # 连续站上/跌破 MA20 天数
        green_streak = 0
        red_streak = 0
        for i in range(len(df) - 1, -1, -1):
            row = df.iloc[i]
            price = float(row["close"])
            ma = row["MA20"]
            if pd.notna(ma):
                if price >= ma:
                    green_streak += 1
                else:
                    red_streak += 1
                if green_streak + red_streak >= self.red_days:
                    break

        # 连续站上/跌破 MA60 天数
        above_ma60_days = 0
        below_ma60_days = 0
        for i in range(len(df) - 1, -1, -1):
            row = df.iloc[i]
            price = float(row["close"])
            ma = row["MA60"]
            if pd.notna(ma):
                if price >= ma:
                    above_ma60_days += 1
                else:
                    below_ma60_days += 1
                if above_ma60_days + below_ma60_days >= self.clear_days:
                    break

        # 今日涨跌幅（用日线数据的最新 pct_change）
        change_pct = float(df.iloc[-1].get("pct_change", 0) * 100) if "pct_change" in df.columns else 0

        payload = {
            "name": index_name,
            "symbol": symbol,
            "market_code": market_code,
            "as_of_date": str(latest.get("date", datetime.now().strftime("%Y-%m-%d"))),
            "close": close,
            "ma20": ma20,
            "ma60": ma60,
            "ma20_pct": ma20_pct,
            "ma60_pct": ma60_pct,
            "above_ma20": close >= ma20 if ma20 else False,
            "above_ma60_days": above_ma60_days,
            "below_ma60_days": below_ma60_days,
            "green_streak": green_streak,
            "red_streak": red_streak,
            "change_pct": change_pct,
            "signal": "CLEAR" if below_ma60_days >= self.clear_days else ("GREEN" if close >= ma20 else "RED"),
            "source": "akshare",
            "source_chain": ["mx_data_failed", "akshare"],
            "stale": False,
        }
        save_json_cache("market_timer", cache_key, payload, meta={"source": "akshare"})
        # 补充实时涨跌幅
        rt = self._fetch_realtime(symbol)
        if rt:
            payload["close"] = rt["price"]
            payload["change_pct"] = rt["chg_pct"]
        return payload


# ---------------------------------------------------------------------------
# 便捷函数
# ---------------------------------------------------------------------------

_timer_instance = None


def get_signal() -> Literal["GREEN", "YELLOW", "RED", "CLEAR"]:
    """获取大盘信号（全局单例）"""
    global _timer_instance
    if _timer_instance is None:
        _timer_instance = MarketTimer()
    return _timer_instance.get_signal()


def get_detail() -> dict:
    """获取大盘详细数据"""
    global _timer_instance
    if _timer_instance is None:
        _timer_instance = MarketTimer()
    return _timer_instance.get_detail()


def get_position_multiplier() -> float:
    """获取仓位系数"""
    global _timer_instance
    if _timer_instance is None:
        _timer_instance = MarketTimer()
    return _timer_instance.get_position_multiplier()


def load_market_snapshot() -> dict:
    """统一市场快照读接口"""
    global _timer_instance
    if _timer_instance is None:
        _timer_instance = MarketTimer()
    return _timer_instance.get_snapshot()


if __name__ == "__main__":
    print("大盘择时判断")
    print("=" * 40)
    snapshot = load_market_snapshot()
    signal = snapshot.get("signal", "CLEAR")
    detail = snapshot.get("indices", {})
    print(f"信号: {signal}")
    print(f"仓位系数: {get_position_multiplier()}")
    print(f"来源: {snapshot.get('source', '')} / {' > '.join(snapshot.get('source_chain', []))}")
    print()
    for name, data in detail.items():
        if isinstance(data, dict) and "error" not in data:
            print(f"{name}:")
            print(f"  收盘: {data.get('close', 'N/A')}")
            print(f"  MA20: {data.get('ma20', 'N/A')} | {'✅站上' if data.get('above_ma20') else '❌跌破'}")
            print(f"  MA60: {data.get('ma60', 'N/A')}")
            print(f"  连续站上MA20: {data.get('green_streak', 0)}日")
            print(f"  连续跌破MA20: {data.get('red_streak', 0)}日")
            print(f"  MA60下方天数: {data.get('below_ma60_days', 0)}日")
