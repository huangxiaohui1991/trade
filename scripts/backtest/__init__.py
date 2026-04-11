"""Backtest and walk-forward helpers."""

from scripts.backtest.runner import (
    compare_backtest_history,
    list_backtest_history,
    load_backtest_inputs,
    run_backtest,
    run_parameter_sweep,
    run_walk_forward,
)
from scripts.backtest.strategy_replay import run_strategy_replay
from scripts.backtest.drawdown import run_drawdown_analysis
from scripts.backtest.historical_pipeline import (
    build_replay_fixture,
    compare_system_strategy_presets,
    diagnose_signal_snapshot,
    render_signal_snapshot_diagnosis_report,
    run_multi_stock_system_backtest,
    run_single_stock_strategy_validation,
    run_system_strategy_backtest,
)

__all__ = [
    "compare_backtest_history",
    "compare_system_strategy_presets",
    "diagnose_signal_snapshot",
    "list_backtest_history",
    "load_backtest_inputs",
    "render_signal_snapshot_diagnosis_report",
    "run_backtest",
    "run_drawdown_analysis",
    "run_multi_stock_system_backtest",
    "run_parameter_sweep",
    "run_single_stock_strategy_validation",
    "run_strategy_replay",
    "run_system_strategy_backtest",
    "run_walk_forward",
    "build_replay_fixture",
]
