"""Tests for strategy/timer.py — pure function market timing"""

import pytest

from hermes.strategy.models import MarketSignal
from hermes.strategy.timer import compute_market_signal


def test_green_signal():
    index_data = {
        "上证指数": {"above_ma20": True, "below_ma60_days": 0},
        "深证成指": {"above_ma20": True, "below_ma60_days": 0},
        "创业板指": {"above_ma20": True, "below_ma60_days": 0},
    }
    state = compute_market_signal(index_data)
    assert state.signal == MarketSignal.GREEN
    assert state.multiplier == 1.0


def test_red_signal():
    index_data = {
        "上证指数": {"above_ma20": False, "below_ma60_days": 3},
        "深证成指": {"above_ma20": False, "below_ma60_days": 5},
        "创业板指": {"above_ma20": False, "below_ma60_days": 2},
    }
    state = compute_market_signal(index_data)
    assert state.signal == MarketSignal.RED
    assert state.multiplier == 0.0


def test_yellow_signal():
    index_data = {
        "上证指数": {"above_ma20": True, "below_ma60_days": 0},
        "深证成指": {"above_ma20": False, "below_ma60_days": 3},
        "创业板指": {"above_ma20": False, "below_ma60_days": 2},
    }
    state = compute_market_signal(index_data)
    assert state.signal == MarketSignal.YELLOW
    assert state.multiplier == 0.5


def test_clear_signal():
    index_data = {
        "上证指数": {"above_ma20": False, "below_ma60_days": 20},
        "深证成指": {"above_ma20": False, "below_ma60_days": 18},
        "创业板指": {"above_ma20": False, "below_ma60_days": 25},
    }
    state = compute_market_signal(index_data)
    assert state.signal == MarketSignal.CLEAR
    assert state.multiplier == 0.0


def test_no_data():
    state = compute_market_signal({})
    assert state.signal == MarketSignal.CLEAR
    assert state.multiplier == 0.0


def test_partial_error():
    index_data = {
        "上证指数": {"above_ma20": True, "below_ma60_days": 0},
        "深证成指": {"error": "timeout"},
        "创业板指": {"above_ma20": True, "below_ma60_days": 0},
    }
    state = compute_market_signal(index_data)
    assert state.signal == MarketSignal.GREEN  # 2/2 valid are green


def test_custom_clear_days():
    index_data = {
        "上证指数": {"above_ma20": False, "below_ma60_days": 8},
        "深证成指": {"above_ma20": False, "below_ma60_days": 10},
        "创业板指": {"above_ma20": False, "below_ma60_days": 9},
    }
    state = compute_market_signal(index_data, config={"clear_days_ma60": 7})
    assert state.signal == MarketSignal.CLEAR
