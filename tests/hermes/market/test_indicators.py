"""Tests for market/indicators.py — technical indicator computation"""

import pytest
import pandas as pd
import numpy as np

from hermes.market.indicators import compute_technical_indicators, _ma, _rsi
from hermes.market.models import StockQuote


def _make_kline(n: int = 120, base_price: float = 15.0, trend: float = 0.001) -> pd.DataFrame:
    """Generate a synthetic daily kline DataFrame."""
    dates = pd.date_range("2026-01-01", periods=n, freq="B")
    prices = [base_price]
    for i in range(1, n):
        change = np.random.normal(trend, 0.015)
        prices.append(prices[-1] * (1 + change))

    df = pd.DataFrame({
        "日期": dates.strftime("%Y-%m-%d"),
        "开盘": [p * 0.998 for p in prices],
        "收盘": prices,
        "最高": [p * 1.01 for p in prices],
        "最低": [p * 0.99 for p in prices],
        "成交量": [5000000 + np.random.randint(-1000000, 1000000) for _ in range(n)],
        "成交额": [p * 5000000 for p in prices],
        "涨跌幅": [0] + [((prices[i] - prices[i-1]) / prices[i-1]) * 100 for i in range(1, n)],
    })
    return df


def test_basic_computation():
    df = _make_kline(120)
    result = compute_technical_indicators(df)

    assert result.ma5 > 0
    assert result.ma10 > 0
    assert result.ma20 > 0
    assert result.ma60 > 0
    assert 0 <= result.rsi <= 100
    assert result.volume_ratio > 0


def test_empty_kline():
    result = compute_technical_indicators(pd.DataFrame())
    assert result.ma5 == 0.0
    assert result.rsi == 50.0  # default RSI when no data


def test_none_kline():
    result = compute_technical_indicators(None)
    assert result.ma5 == 0.0


def test_short_kline():
    """Kline shorter than MA window should return 0 for that MA."""
    df = _make_kline(15)
    result = compute_technical_indicators(df)

    assert result.ma5 > 0
    assert result.ma10 > 0
    assert result.ma20 == 0.0  # need 20 bars
    assert result.ma60 == 0.0  # need 60 bars


def test_golden_cross_detection():
    """Build a kline where MA5 crosses above MA10 on the last bar."""
    n = 30
    # Start with declining prices (MA5 < MA10), then sharp upturn
    prices = [20.0 - i * 0.1 for i in range(20)]  # declining
    prices += [prices[-1] + i * 0.3 for i in range(1, 11)]  # sharp upturn

    df = pd.DataFrame({
        "日期": pd.date_range("2026-01-01", periods=n, freq="B").strftime("%Y-%m-%d"),
        "开盘": prices,
        "收盘": prices,
        "最高": [p * 1.01 for p in prices],
        "最低": [p * 0.99 for p in prices],
        "成交量": [5000000] * n,
        "成交额": [p * 5000000 for p in prices],
        "涨跌幅": [0] + [((prices[i] - prices[i-1]) / prices[i-1]) * 100 for i in range(1, n)],
    })

    result = compute_technical_indicators(df)
    # After sharp upturn, MA5 should be above MA10
    assert result.ma5 > result.ma10


def test_above_ma20_uses_quote():
    """When quote is provided, above_ma20 should use quote.close, not kline close."""
    df = _make_kline(60, base_price=15.0, trend=0.001)
    # Quote with price well above MA20
    quote = StockQuote(
        code="002138", name="test", price=20.0,
        open=19.5, high=20.5, low=19.0, close=20.0,
        volume=5000000, amount=1e8, change_pct=2.0,
    )
    result = compute_technical_indicators(df, quote)
    assert result.above_ma20 is True
    assert result.change_pct == 2.0


def test_english_column_names():
    """Should handle English column names (stock_zh_a_daily format)."""
    n = 60
    prices = [15.0 + i * 0.05 for i in range(n)]
    df = pd.DataFrame({
        "date": pd.date_range("2026-01-01", periods=n, freq="B").strftime("%Y-%m-%d"),
        "open": [p * 0.998 for p in prices],
        "close": prices,
        "high": [p * 1.01 for p in prices],
        "low": [p * 0.99 for p in prices],
        "volume": [5000000] * n,
        "amount": [p * 5000000 for p in prices],
        "pct_change": [0] + [((prices[i] - prices[i-1]) / prices[i-1]) * 100 for i in range(1, n)],
    })
    result = compute_technical_indicators(df)
    assert result.ma20 > 0
    assert result.ma5 > 0


# ── helper function tests ──

def test_ma_helper():
    s = pd.Series([10, 11, 12, 13, 14])
    assert _ma(s, 3) == pytest.approx((12 + 13 + 14) / 3, rel=1e-6)
    assert _ma(s, 10) == 0.0  # not enough data


def test_rsi_helper():
    # All gains → RSI should be 100
    s = pd.Series(range(20))
    assert _rsi(s, 14) == 100.0

    # All losses → RSI should be near 0
    s = pd.Series([20 - i for i in range(20)])
    assert _rsi(s, 14) < 5.0
