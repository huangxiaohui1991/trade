"""Tests for historical strategy fixture parameter resolution."""

import unittest
from unittest.mock import patch

import pandas as pd

from scripts.backtest import historical_pipeline
from scripts.backtest.historical_pipeline import (
    _entry_signal_for_mode,
    _fetch_daily,
    _parse_symbol_list,
    _resolve_strategy_params,
    render_single_stock_validation_report,
    compare_system_strategy_presets,
    run_single_stock_strategy_validation,
    run_multi_stock_system_backtest,
)
from scripts.utils.config_loader import clear_config_cache


class HistoricalPipelineTests(unittest.TestCase):

    def setUp(self):
        clear_config_cache("strategy")
        historical_pipeline._FETCH_DAILY_CACHE.clear()
        historical_pipeline._BS_SESSION_OPEN = False

    def test_aggressive_preset_applies_and_custom_override_wins(self):
        params = _resolve_strategy_params({
            "preset": "aggressive_high_return",
            "buy_threshold": 6.2,
        })

        self.assertEqual(params["preset"], "aggressive_high_return")
        self.assertEqual(params["entry_mode"], "hybrid")
        self.assertTrue(params["require_entry_signal"])
        self.assertEqual(params["buy_threshold"], 6.2)
        self.assertAlmostEqual(params["momentum_stop_loss"], 0.08)
        self.assertAlmostEqual(params["momentum_trailing_stop"], 0.10)
        self.assertEqual(params["momentum_time_stop_days"], 10)
        self.assertAlmostEqual(params["stop_loss"], 0.08)
        self.assertAlmostEqual(params["take_profit"], 0.10)
        self.assertEqual(params["time_stop_days"], 10)

    def test_score_only_mode_disables_entry_requirement_by_default(self):
        params = _resolve_strategy_params({"entry_mode": "score_only"})
        self.assertFalse(params["require_entry_signal"])

    def test_score_capture_preset_disables_entry_requirement(self):
        params = _resolve_strategy_params({"preset": "aggressive_score_capture"})
        self.assertEqual(params["entry_mode"], "score_only")
        self.assertFalse(params["require_entry_signal"])
        self.assertAlmostEqual(params["momentum_trailing_stop"], 0.10)

    def test_hybrid_entry_mode_accepts_trend_and_pullback_setups(self):
        row = pd.Series({
            "close": 20.0,
            "MA5": 19.8,
            "MA10": 19.5,
            "MA20": 18.6,
            "MA60": 17.2,
        })
        tech = {
            "golden_cross": False,
            "volume_ratio": 1.1,
            "rsi": 72.0,
        }

        entry_signal, reasons = _entry_signal_for_mode(
            row,
            tech,
            {
                "entry_mode": "hybrid",
                "entry_rsi_max": 70,
                "entry_trend_rsi_max": 75,
                "entry_pullback_rsi_max": 75,
                "entry_pullback_volume_ratio_min": 1.0,
            },
        )

        self.assertTrue(entry_signal)
        self.assertEqual(reasons, ["trend_follow", "pullback"])

    def test_fetch_daily_reuses_in_memory_cache(self):
        class _FakeLoginResult:
            error_code = "0"
            error_msg = ""

        class _FakeQueryResult:
            error_code = "0"
            error_msg = ""
            fields = ["date", "open", "high", "low", "close", "volume", "amount", "turn"]

            def __init__(self):
                self._rows = iter([
                    ["2026-04-01", "10", "10.5", "9.8", "10.2", "1000", "10200", "1.5"],
                ])
                self._current = None

            def next(self):
                try:
                    self._current = next(self._rows)
                    return True
                except StopIteration:
                    return False

            def get_row_data(self):
                return self._current

        with (
            patch.object(historical_pipeline.bs, "login", return_value=_FakeLoginResult()) as mock_login,
            patch.object(historical_pipeline.bs, "query_history_k_data_plus", return_value=_FakeQueryResult()) as mock_query,
        ):
            df1 = _fetch_daily("sh.000001", "2026-04-01", "2026-04-10")
            df2 = _fetch_daily("sh.000001", "2026-04-01", "2026-04-10")

        self.assertEqual(mock_login.call_count, 1)
        self.assertEqual(mock_query.call_count, 1)
        self.assertEqual(len(df1), 1)
        self.assertEqual(len(df2), 1)
        self.assertFalse(df1 is df2)

    def test_parse_symbol_list_supports_slash_and_comma(self):
        self.assertEqual(
            _parse_symbol_list("601127/002962, 601869 002851"),
            ["601127", "002962", "601869", "002851"],
        )

    def test_run_multi_stock_system_backtest_aggregates(self):
        fake_results = {
            "AAA": {
                "summary": {
                    "closed_trade_count": 2,
                    "win_count": 1,
                    "loss_count": 1,
                    "win_rate": 50.0,
                    "total_realized_pnl": 1000.0,
                    "ending_equity": 101000.0,
                    "max_drawdown_pct": -2.5,
                    "max_drawdown_date": "2026-04-02",
                    "simulation_mode": "system_strategy_replay",
                },
                "params": {"entry_mode": "hybrid", "preset": "aggressive_high_return"},
            },
            "BBB": {
                "summary": {
                    "closed_trade_count": 1,
                    "win_count": 1,
                    "loss_count": 0,
                    "win_rate": 100.0,
                    "total_realized_pnl": 500.0,
                    "ending_equity": 100500.0,
                    "max_drawdown_pct": -1.0,
                    "max_drawdown_date": "2026-04-03",
                    "simulation_mode": "system_strategy_replay",
                },
                "params": {"entry_mode": "hybrid", "preset": "aggressive_high_return"},
            },
        }

        with patch.object(historical_pipeline, "run_system_strategy_backtest", side_effect=lambda stock_code, **_: fake_results[stock_code]):
            result = run_multi_stock_system_backtest(
                stock_codes=["AAA", "BBB"],
                start="2026-04-01",
                end="2026-04-10",
                strategy_params={"preset": "aggressive_high_return"},
            )

        self.assertEqual(result["action"], "system_strategy_batch_replay")
        self.assertEqual(result["aggregate"]["stock_count"], 2)
        self.assertEqual(result["aggregate"]["closed_trade_count"], 3)
        self.assertEqual(result["aggregate"]["total_realized_pnl"], 1500.0)
        self.assertEqual(result["aggregate"]["blended_win_rate"], 66.7)
        self.assertEqual(result["aggregate"]["worst_max_drawdown_pct"], -2.5)

    def test_build_replay_fixture_prefers_history_signal_snapshots(self):
        df_index = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-04-01", "2026-04-02", "2026-04-03"]),
                "open": [3000.0, 3010.0, 3020.0],
                "high": [3010.0, 3020.0, 3030.0],
                "low": [2990.0, 3000.0, 3010.0],
                "close": [3005.0, 3015.0, 3025.0],
                "volume": [1, 1, 1],
                "amount": [700_000_000_000, 710_000_000_000, 720_000_000_000],
                "turn": [1.0, 1.0, 1.0],
            }
        )
        df_stock = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-04-01", "2026-04-02", "2026-04-03"]),
                "open": [10.0, 10.5, 11.0],
                "high": [10.4, 10.8, 11.4],
                "low": [9.8, 10.1, 10.7],
                "close": [10.2, 10.6, 11.2],
                "volume": [1000, 1200, 1400],
                "amount": [10_200, 12_720, 15_680],
                "turn": [1.1, 1.2, 1.3],
            }
        )
        params = {
            "technical_weight": 3,
            "fundamental_weight": 2,
            "flow_weight": 2,
            "sentiment_weight": 3,
            "technical_denom": 3,
            "fundamental_denom": 2,
            "flow_denom": 2,
            "sentiment_denom": 3,
            "entry_mode": "hybrid",
            "require_entry_signal": True,
            "use_history_snapshots": True,
        }

        def _bundle(day: str) -> dict:
            if day == "2026-04-01":
                return {
                    "status": "ok",
                    "history_group_id": "grp-001",
                    "market_snapshot": {"signal": "RED"},
                    "candidate_snapshot": {"candidate_count": 1},
                    "scored_candidates": [
                        {
                            "code": "AAA",
                            "name": "AAA Snapshot",
                            "total_score": 9.1,
                            "technical_score": 2.8,
                            "fundamental_score": 1.7,
                            "flow_score": 1.4,
                            "sentiment_score": 2.2,
                            "entry_signal": True,
                            "veto_signals": ["red_market"],
                            "style": "momentum",
                            "price": 10.2,
                        }
                    ],
                }
            if day == "2026-04-02":
                return {
                    "status": "ok",
                    "history_group_id": "grp-002",
                    "market_snapshot": {"signal": "GREEN"},
                    "candidate_snapshot": {"candidate_count": 0},
                    "scored_candidates": [],
                }
            return {
                "status": "missing",
                "history_group_id": "",
                "market_snapshot": {},
                "candidate_snapshot": {},
                "scored_candidates": [],
            }

        with (
            patch.object(historical_pipeline, "_resolve_strategy_params", return_value=params),
            patch.object(historical_pipeline, "get_strategy", return_value={}),
            patch.object(historical_pipeline, "_fetch_daily", side_effect=[df_index, df_stock]),
            patch.object(historical_pipeline, "load_daily_signal_snapshot_bundle", side_effect=_bundle),
        ):
            fixture = historical_pipeline.build_replay_fixture(
                stock_code="AAA",
                start="2026-04-01",
                end="2026-04-03",
            )

        self.assertEqual(fixture["daily_data"]["2026-04-01"]["market_signal"], "RED")
        self.assertEqual(fixture["daily_data"]["2026-04-01"]["snapshot_source"], "history_signal_snapshot")
        self.assertEqual(fixture["daily_data"]["2026-04-01"]["candidates"][0]["score"], 9.1)
        self.assertEqual(fixture["daily_data"]["2026-04-02"]["snapshot_source"], "history_signal_snapshot")
        self.assertEqual(fixture["daily_data"]["2026-04-02"]["candidates"], [])
        self.assertEqual(fixture["daily_data"]["2026-04-03"]["snapshot_source"], "proxy_replay")
        self.assertEqual(fixture["_meta"]["data_fidelity"]["mode"], "hybrid_signal_mirror")
        self.assertEqual(fixture["_meta"]["data_fidelity"]["history_days"], 2)
        self.assertEqual(fixture["_meta"]["data_fidelity"]["proxy_days"], 1)
        self.assertEqual(fixture["_meta"]["data_fidelity"]["history_candidate_absent_days"], 1)

    def test_compare_system_strategy_presets_ranks_by_total_pnl(self):
        def _fake_batch(stock_codes, start, end, index_code="system", total_capital=None, strategy_params=None):
            preset = strategy_params["preset"]
            if preset == "better":
                aggregate = {
                    "stock_count": 2,
                    "closed_trade_count": 5,
                    "total_realized_pnl": 3000.0,
                    "avg_ending_equity": 103000.0,
                    "worst_max_drawdown_pct": -4.0,
                    "blended_win_rate": 60.0,
                    "entry_mode": "hybrid",
                    "preset": preset,
                }
            else:
                aggregate = {
                    "stock_count": 2,
                    "closed_trade_count": 6,
                    "total_realized_pnl": 2000.0,
                    "avg_ending_equity": 102000.0,
                    "worst_max_drawdown_pct": -3.0,
                    "blended_win_rate": 55.0,
                    "entry_mode": "score_only",
                    "preset": preset,
                }
            return {
                "aggregate": aggregate,
                "results": [{"code": "AAA"}, {"code": "BBB"}],
                "params": {
                    "entry_mode": aggregate["entry_mode"],
                    "preset": preset,
                    "buy_threshold": 6.0,
                    "momentum_stop_loss": 0.08,
                    "momentum_trailing_stop": 0.10,
                    "momentum_time_stop_days": 10,
                },
            }

        with patch.object(historical_pipeline, "run_multi_stock_system_backtest", side_effect=_fake_batch):
            result = compare_system_strategy_presets(
                stock_codes=["AAA", "BBB"],
                preset_names=["worse", "better"],
                start="2026-04-01",
                end="2026-04-10",
            )

        self.assertEqual(result["action"], "system_strategy_preset_compare")
        self.assertEqual(result["ranked"][0]["preset"], "better")
        self.assertEqual(result["ranked"][1]["preset"], "worse")
        self.assertEqual(result["code_breakdown"]["better"], [{"code": "AAA"}, {"code": "BBB"}])

    def test_single_stock_validation_highlights_missed_window(self):
        fixture = {
            "daily_data": {
                "2026-04-01": {
                    "market_signal": "GREEN",
                    "candidates": [{
                        "code": "AAA",
                        "name": "A",
                        "score": 8.0,
                        "price": 10.0,
                        "entry_signal": False,
                        "entry_reasons": [],
                        "veto_signals": [],
                    }],
                    "prices": {"AAA": 10.0},
                    "bars": {"AAA": {"close": 10.0, "high": 10.0, "low": 9.8}},
                },
                "2026-04-02": {
                    "market_signal": "GREEN",
                    "candidates": [{
                        "code": "AAA",
                        "name": "A",
                        "score": 8.1,
                        "price": 12.0,
                        "entry_signal": False,
                        "entry_reasons": [],
                        "veto_signals": [],
                    }],
                    "prices": {"AAA": 12.0},
                    "bars": {"AAA": {"close": 12.0, "high": 12.0, "low": 11.7}},
                },
                "2026-04-03": {
                    "market_signal": "GREEN",
                    "candidates": [{
                        "code": "AAA",
                        "name": "A",
                        "score": 8.2,
                        "price": 15.0,
                        "entry_signal": False,
                        "entry_reasons": [],
                        "veto_signals": [],
                    }],
                    "prices": {"AAA": 15.0},
                    "bars": {"AAA": {"close": 15.0, "high": 15.0, "low": 14.8}},
                },
            },
            "total_capital": 100000,
            "params": {
                "buy_threshold": 7,
                "require_entry_signal": True,
                "entry_mode": "hybrid",
            },
            "_meta": {"stock_code": "AAA"},
        }
        replay = {
            "summary": {
                "total_realized_pnl": 0.0,
                "ending_equity": 100000.0,
                "closed_trade_count": 0,
                "win_rate": 0.0,
                "max_drawdown_pct": 0.0,
                "max_drawdown_date": "",
            },
            "closed_trades": [],
            "open_positions": [],
            "rejected_entries": [
                {"code": "AAA", "date": "2026-04-01", "score": 8.0, "reason": "entry_signal_missing"},
            ],
            "timeline": [],
        }

        with (
            patch.object(historical_pipeline, "build_replay_fixture", return_value=fixture),
            patch.object(historical_pipeline, "_resolve_strategy_params", return_value=fixture["params"]),
            patch("scripts.backtest.strategy_replay.run_strategy_replay", return_value=replay),
        ):
            result = run_single_stock_strategy_validation(
                stock_code="AAA",
                start="2026-04-01",
                end="2026-04-03",
            )

        self.assertEqual(result["action"], "single_stock_strategy_validation")
        self.assertEqual(result["data_fidelity"]["mode"], "proxy_replay")
        self.assertEqual(result["diagnostics"]["opportunity_statistics"]["total_opportunity_windows"], 1)
        self.assertEqual(result["diagnostics"]["opportunity_statistics"]["captured_opportunity_windows"], 0)
        self.assertEqual(
            result["diagnostics"]["opportunity_miss_reason_breakdown"][0]["reason"],
            "entry_signal_missing",
        )

    def test_single_stock_validation_reports_fixture_data_fidelity(self):
        fixture = {
            "daily_data": {
                "2026-04-01": {
                    "market_signal": "GREEN",
                    "candidates": [{"code": "AAA", "score": 8.0, "price": 10.0, "entry_signal": True, "veto_signals": []}],
                    "prices": {"AAA": 10.0},
                    "bars": {"AAA": {"close": 10.0, "high": 10.0, "low": 9.8}},
                }
            },
            "total_capital": 100000,
            "params": {"buy_threshold": 7, "require_entry_signal": True, "entry_mode": "hybrid"},
            "_meta": {
                "stock_code": "AAA",
                "data_fidelity": {
                    "mode": "hybrid_signal_mirror",
                    "history_days": 2,
                    "proxy_days": 1,
                    "history_candidate_absent_days": 1,
                },
            },
        }
        replay = {
            "summary": {
                "total_realized_pnl": 0.0,
                "ending_equity": 100000.0,
                "closed_trade_count": 0,
                "win_rate": 0.0,
                "max_drawdown_pct": 0.0,
                "max_drawdown_date": "",
            },
            "closed_trades": [],
            "open_positions": [],
            "rejected_entries": [],
            "timeline": [],
        }

        with (
            patch.object(historical_pipeline, "build_replay_fixture", return_value=fixture),
            patch.object(historical_pipeline, "_resolve_strategy_params", return_value=fixture["params"]),
            patch("scripts.backtest.strategy_replay.run_strategy_replay", return_value=replay),
        ):
            result = run_single_stock_strategy_validation(
                stock_code="AAA",
                start="2026-04-01",
                end="2026-04-01",
            )

        self.assertEqual(result["data_fidelity"]["mode"], "hybrid_signal_mirror")
        self.assertIn("2 个交易日使用历史信号快照", result["data_fidelity"]["notes"][0])

    def test_single_stock_validation_reports_premature_exit(self):
        fixture = {
            "daily_data": {
                "2026-04-01": {
                    "market_signal": "GREEN",
                    "candidates": [{"code": "AAA", "score": 8.0, "price": 10.0, "entry_signal": True, "veto_signals": []}],
                    "prices": {"AAA": 10.0},
                    "bars": {"AAA": {"close": 10.0, "high": 10.0, "low": 9.8}},
                },
                "2026-04-02": {
                    "market_signal": "GREEN",
                    "candidates": [{"code": "AAA", "score": 8.0, "price": 11.0, "entry_signal": True, "veto_signals": []}],
                    "prices": {"AAA": 11.0},
                    "bars": {"AAA": {"close": 11.0, "high": 11.0, "low": 10.7}},
                },
                "2026-04-03": {
                    "market_signal": "GREEN",
                    "candidates": [{"code": "AAA", "score": 8.0, "price": 12.5, "entry_signal": True, "veto_signals": []}],
                    "prices": {"AAA": 12.5},
                    "bars": {"AAA": {"close": 12.5, "high": 12.5, "low": 12.0}},
                },
                "2026-04-04": {
                    "market_signal": "GREEN",
                    "candidates": [{"code": "AAA", "score": 8.0, "price": 13.5, "entry_signal": True, "veto_signals": []}],
                    "prices": {"AAA": 13.5},
                    "bars": {"AAA": {"close": 13.5, "high": 13.5, "low": 13.2}},
                },
            },
            "total_capital": 100000,
            "params": {"buy_threshold": 7, "require_entry_signal": True, "entry_mode": "hybrid"},
            "_meta": {"stock_code": "AAA"},
        }
        replay = {
            "summary": {
                "total_realized_pnl": 1000.0,
                "ending_equity": 101000.0,
                "closed_trade_count": 1,
                "win_rate": 100.0,
                "max_drawdown_pct": -1.0,
                "max_drawdown_date": "2026-04-02",
            },
            "closed_trades": [
                {
                    "code": "AAA",
                    "entry_date": "2026-04-01",
                    "exit_date": "2026-04-02",
                    "entry_price": 10.0,
                    "exit_price": 11.0,
                    "realized_pnl": 1000.0,
                    "exit_reason": "system_ma20_exit",
                    "holding_days": 1,
                }
            ],
            "open_positions": [],
            "rejected_entries": [],
            "timeline": [{"date": "2026-04-01", "entry_count": 1}, {"date": "2026-04-02", "entry_count": 0}],
        }

        with (
            patch.object(historical_pipeline, "build_replay_fixture", return_value=fixture),
            patch.object(historical_pipeline, "_resolve_strategy_params", return_value=fixture["params"]),
            patch("scripts.backtest.strategy_replay.run_strategy_replay", return_value=replay),
        ):
            result = run_single_stock_strategy_validation(
                stock_code="AAA",
                start="2026-04-01",
                end="2026-04-04",
            )

        self.assertEqual(result["diagnostics"]["premature_exit_count"], 1)
        self.assertEqual(result["premature_exits"][0]["exit_reason"], "system_ma20_exit")
        text = render_single_stock_validation_report(result)
        self.assertIn("提前离场", text)


if __name__ == "__main__":
    unittest.main()
