"""Shared policy for deciding whether a pipeline run should be skipped."""

from __future__ import annotations

from typing import Literal

SkipReason = Literal["non_trading_day", "completed_today"]
DataSourceGateDecision = Literal["failed", "warning"]

MULTI_RUN_PIPELINES = frozenset({"sentiment", "intraday_monitor"})
NON_TRADING_DAY_PIPELINES = frozenset({"sentiment", "weekly", "monthly"})
MARKET_DATA_GATED_PIPELINES = frozenset({
    "morning",
    "noon",
    "intraday_monitor",
    "evening",
    "scoring",
    "auto_trade",
})


def should_skip_pipeline(
    pipeline_type: str,
    *,
    is_trading_day: bool,
    is_completed_today: bool,
) -> SkipReason | None:
    """Return a skip reason for pipeline execution, or None when it may run."""
    if not is_trading_day and pipeline_type not in NON_TRADING_DAY_PIPELINES:
        return "non_trading_day"
    if is_completed_today and pipeline_type not in MULTI_RUN_PIPELINES:
        return "completed_today"
    return None


def data_source_gate_decision(
    pipeline_type: str,
    data_source_health: dict,
) -> DataSourceGateDecision | None:
    """Return data-source gate decision for pipelines that depend on market data."""
    if pipeline_type not in MARKET_DATA_GATED_PIPELINES:
        return None
    status = data_source_health.get("status")
    if status == "failed":
        return "failed"
    if status == "warning":
        return "warning"
    return None
