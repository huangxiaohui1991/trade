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

from scripts.utils.config_loader import get_strategy
from scripts.utils.logger import get_logger

_logger = get_logger("market_timer")


# ---------------------------------------------------------------------------
# 大盘指数配置
# ---------------------------------------------------------------------------
_INDICES = {
    "上证指数": "sh000001",
    "深证成指": "sz399001",
    "创业板指": "sz399006",
    "科创50": "sh000688",
}


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

    # ---------------------------------------------------------------------------
    # 公开接口
    # ---------------------------------------------------------------------------

    def get_signal(self) -> Literal["GREEN", "YELLOW", "RED", "CLEAR"]:
        """返回大盘信号"""
        self._compute()
        return self._signal

    def get_detail(self) -> dict:
        """返回详细数据（每次调用都会重新计算）"""
        self._compute()
        return self._detail

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

    def _compute(self) -> None:
        """计算大盘信号"""
        index_data = {}
        for name, symbol in _INDICES.items():
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

    def _fetch_index_data(self, symbol: str) -> dict:
        """
        获取单个指数的技术数据（MX优先 → akshare fallback）
        """
        # MX 优先：查指数历史数据
        try:
            from scripts.mx.mx_data import MXData
            index_names = {"sh000001": "上证指数", "sz399001": "深证成指",
                           "sz399006": "创业板指", "sh000688": "科创50"}
            idx_name = index_names.get(symbol, symbol)
            mx = MXData()
            result = mx.query(f"{idx_name} 近80个交易日收盘价")
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

                _logger.info(f"[market_timer] MX 成功 {idx_name}: close={close} ma20={ma20}")
                return {
                    "close": close,
                    "ma20": round(ma20, 2) if ma20 else None,
                    "ma60": round(ma60, 2) if ma60 else None,
                    "above_ma20": close >= ma20 if ma20 else False,
                    "above_ma60_days": above_ma60_days,
                    "below_ma60_days": below_ma60_days,
                    "green_streak": green_streak,
                    "red_streak": red_streak,
                    "change_pct": 0,
                }
        except Exception as e:
            _logger.info(f"[market_timer] MX 失败 {symbol}: {e}")

        # akshare fallback
        df = ak.stock_zh_index_daily(symbol=symbol)
        if df is None or df.empty:
            raise ValueError(f"无法获取 {symbol} 数据")

        df = df.sort_values("date").tail(80).copy()
        df["MA20"] = df["close"].rolling(20).mean()
        df["MA60"] = df["close"].rolling(60).mean()

        latest = df.iloc[-1]
        close = float(latest["close"])
        ma20 = float(latest["MA20"]) if pd.notna(latest["MA20"]) else None
        ma60 = float(latest["MA60"]) if pd.notna(latest["MA60"]) else None

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

        return {
            "close": close,
            "ma20": ma20,
            "ma60": ma60,
            "above_ma20": close >= ma20 if ma20 else False,
            "above_ma60_days": above_ma60_days,
            "below_ma60_days": below_ma60_days,
            "green_streak": green_streak,
            "red_streak": red_streak,
            "change_pct": change_pct,
        }


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


if __name__ == "__main__":
    print("大盘择时判断")
    print("=" * 40)
    signal = get_signal()
    detail = get_detail()
    print(f"信号: {signal}")
    print(f"仓位系数: {get_position_multiplier()}")
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
