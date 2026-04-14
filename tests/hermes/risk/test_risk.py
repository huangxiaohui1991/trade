"""Tests for risk/rules.py and risk/sizing.py — pure functions"""

import pytest
from datetime import date

from hermes.risk.models import RiskParams
from hermes.risk.rules import check_exit_signals, check_portfolio_risk, get_risk_params
from hermes.risk.sizing import calc_position_size
from hermes.strategy.models import Style


def test_stop_loss_triggered():
    signals = check_exit_signals(
        code="002138", avg_cost=50.0, current_price=45.0,
        entry_date=date(2026, 4, 1), today=date(2026, 4, 10),
        highest_since_entry=52.0, entry_day_low=49.0,
        params=RiskParams(style=Style.MOMENTUM, stop_loss=0.08),
    )
    stop_signals = [s for s in signals if s.signal_type == "stop_loss"]
    assert len(stop_signals) >= 1
    assert stop_signals[0].urgency == "immediate"


def test_stop_loss_not_triggered():
    signals = check_exit_signals(
        code="002138", avg_cost=50.0, current_price=48.0,
        entry_date=date(2026, 4, 1), today=date(2026, 4, 10),
        highest_since_entry=52.0, entry_day_low=49.0,
        params=RiskParams(style=Style.MOMENTUM, stop_loss=0.08),
    )
    stop_signals = [s for s in signals if s.signal_type == "stop_loss"]
    assert len(stop_signals) == 0


def test_trailing_stop_triggered():
    signals = check_exit_signals(
        code="002138", avg_cost=50.0, current_price=49.0,
        entry_date=date(2026, 4, 1), today=date(2026, 4, 10),
        highest_since_entry=56.0, entry_day_low=49.0,
        params=RiskParams(style=Style.MOMENTUM, trailing_stop=0.10),
    )
    trail = [s for s in signals if s.signal_type == "trailing_stop"]
    assert len(trail) == 1
    assert trail[0].trigger_price == round(56.0 * 0.9, 2)


def test_time_stop_triggered():
    signals = check_exit_signals(
        code="002138", avg_cost=50.0, current_price=50.5,
        entry_date=date(2026, 3, 1), today=date(2026, 4, 10),
        highest_since_entry=51.0, entry_day_low=49.0,
        params=RiskParams(style=Style.MOMENTUM, time_stop_days=15),
    )
    time_signals = [s for s in signals if s.signal_type == "time_stop"]
    assert len(time_signals) == 1
    assert time_signals[0].urgency == "advisory"


def test_ma_exit_triggered():
    signals = check_exit_signals(
        code="002138", avg_cost=50.0, current_price=13.5,
        entry_date=date(2026, 4, 1), today=date(2026, 4, 10),
        highest_since_entry=52.0, entry_day_low=49.0,
        params=RiskParams(style=Style.SLOW_BULL, exit_ma=20),
        ma20=14.0,
    )
    ma_signals = [s for s in signals if s.signal_type == "ma_exit"]
    assert len(ma_signals) == 1


def test_get_risk_params_slow_bull():
    p = get_risk_params(Style.SLOW_BULL, {"slow_bull": {"stop_loss": 0.08, "time_stop_days": 30}})
    assert p.stop_loss == 0.08
    assert p.time_stop_days == 30
    assert p.trailing_stop is None


def test_get_risk_params_momentum():
    p = get_risk_params(Style.MOMENTUM, {"momentum": {"trailing_stop": 0.10}})
    assert p.trailing_stop == 0.10


def test_portfolio_risk_daily_loss():
    breaches = check_portfolio_risk(
        daily_pnl_pct=-0.04,
        consecutive_loss_days=1,
        max_single_exposure_pct=0.15,
        max_sector_exposure_pct=0.30,
        limits={"daily_loss_limit_pct": 0.03},
    )
    assert any(b.rule == "daily_loss_limit" for b in breaches)


def test_portfolio_risk_no_breach():
    breaches = check_portfolio_risk(
        daily_pnl_pct=-0.01,
        consecutive_loss_days=0,
        max_single_exposure_pct=0.15,
        max_sector_exposure_pct=0.30,
        limits={"daily_loss_limit_pct": 0.03, "consecutive_loss_days_limit": 2},
    )
    assert len(breaches) == 0


# ── sizing tests ──

def test_position_size_basic():
    ps = calc_position_size(
        total_capital=450000, current_exposure_pct=0.2,
        price=15.0, market_multiplier=1.0,
    )
    assert ps.shares > 0
    assert ps.shares % 100 == 0
    assert ps.pct <= 0.20
    assert ps.amount > 0


def test_position_size_red_market():
    ps = calc_position_size(
        total_capital=450000, current_exposure_pct=0.0,
        price=15.0, market_multiplier=0.0,
    )
    assert ps.shares == 0
    assert ps.amount == 0


def test_position_size_respects_total_limit():
    ps = calc_position_size(
        total_capital=450000, current_exposure_pct=0.55,
        price=15.0, market_multiplier=1.0,
        total_max_pct=0.60,
    )
    assert ps.pct <= 0.05 + 0.001  # only 5% remaining


def test_position_size_yellow_market():
    ps = calc_position_size(
        total_capital=450000, current_exposure_pct=0.0,
        price=15.0, market_multiplier=0.5,
    )
    assert ps.pct <= 0.10 + 0.001  # 20% * 0.5 = 10%
