"""Pipeline run policy tests."""

from __future__ import annotations


def test_intraday_monitor_can_run_more_than_once_on_trading_day():
    from astock_trading.platform.pipeline_policy import should_skip_pipeline

    decision = should_skip_pipeline(
        "intraday_monitor",
        is_trading_day=True,
        is_completed_today=True,
    )

    assert decision is None


def test_weekly_can_run_on_non_trading_day():
    from astock_trading.platform.pipeline_policy import should_skip_pipeline

    decision = should_skip_pipeline(
        "weekly",
        is_trading_day=False,
        is_completed_today=False,
    )

    assert decision is None


def test_trading_day_pipeline_skips_on_non_trading_day():
    from astock_trading.platform.pipeline_policy import should_skip_pipeline

    decision = should_skip_pipeline(
        "morning",
        is_trading_day=False,
        is_completed_today=False,
    )

    assert decision == "non_trading_day"


def test_daily_pipeline_skips_after_successful_run():
    from astock_trading.platform.pipeline_policy import should_skip_pipeline

    decision = should_skip_pipeline(
        "scoring",
        is_trading_day=True,
        is_completed_today=True,
    )

    assert decision == "completed_today"


def test_market_data_pipeline_is_blocked_when_core_data_sources_failed():
    from astock_trading.platform.pipeline_policy import data_source_gate_decision

    decision = data_source_gate_decision(
        "morning",
        {"status": "failed", "required_missing": ["baidu_fund_flow"], "optional_missing": []},
    )

    assert decision == "failed"


def test_market_data_pipeline_continues_when_only_optional_sources_degraded():
    from astock_trading.platform.pipeline_policy import data_source_gate_decision

    decision = data_source_gate_decision(
        "evening",
        {"status": "warning", "required_missing": [], "optional_missing": ["industry_comparison"]},
    )

    assert decision == "warning"


def test_weekly_pipeline_ignores_market_data_source_gate():
    from astock_trading.platform.pipeline_policy import data_source_gate_decision

    decision = data_source_gate_decision(
        "weekly",
        {"status": "failed", "required_missing": ["hot_stocks"], "optional_missing": []},
    )

    assert decision is None
