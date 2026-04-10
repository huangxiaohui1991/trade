"""Tests for extended backtest parameter grid (veto / scoring dimensions)."""

import unittest
from unittest.mock import patch


def _mock_strategy():
    return {
        "scoring": {
            "weights": {"technical": 2, "fundamental": 3, "flow": 2, "sentiment": 3},
            "thresholds": {"buy": 7, "watch": 5, "reject": 4},
            "veto": ["below_ma20", "limit_up_today", "consecutive_outflow", "red_market", "earnings_bomb"],
        },
        "risk": {
            "stop_loss": 0.04,
            "take_profit": {"t1_pct": 0.15},
            "time_stop_days": 15,
            "position": {"total_max": 0.60, "single_max": 0.20, "weekly_max": 2},
            "portfolio": {},
        },
        "capital": 450286,
    }


class ExtendedParameterGridTests(unittest.TestCase):

    @patch("scripts.backtest.runner.get_strategy", side_effect=_mock_strategy)
    def test_baseline_includes_new_fields(self, _mock):
        from scripts.backtest.runner import _baseline_parameters
        baseline = _baseline_parameters()
        self.assertIn("watch_threshold", baseline)
        self.assertIn("reject_threshold", baseline)
        self.assertIn("time_stop_days", baseline)
        self.assertEqual(baseline["watch_threshold"], 5.0)
        self.assertEqual(baseline["reject_threshold"], 4.0)
        self.assertEqual(baseline["time_stop_days"], 15.0)

    @patch("scripts.backtest.runner.get_strategy", side_effect=_mock_strategy)
    def test_grid_includes_veto_presets(self, _mock):
        from scripts.backtest.runner import _parameter_grid
        grid = _parameter_grid(
            buy_thresholds=[7.0],
            stop_losses=[0.04],
            take_profits=[0.15],
            veto_presets=[
                ["below_ma20", "limit_up_today"],
                ["below_ma20"],
            ],
        )
        # 应该有 2 个 veto preset 变体
        veto_sets = [tuple(sorted(p.get("veto_rules", []))) for p in grid]
        unique_veto = set(veto_sets)
        self.assertEqual(len(unique_veto), 2)
        self.assertIn(("below_ma20", "limit_up_today"), unique_veto)
        self.assertIn(("below_ma20",), unique_veto)

    @patch("scripts.backtest.runner.get_strategy", side_effect=_mock_strategy)
    def test_grid_includes_watch_reject_time_stop(self, _mock):
        from scripts.backtest.runner import _parameter_grid
        grid = _parameter_grid(
            buy_thresholds=[7.0],
            stop_losses=[0.04],
            take_profits=[0.15],
            watch_thresholds=[4.0, 5.0],
            reject_thresholds=[3.0, 4.0],
            time_stop_days_list=[10, 15],
        )
        # 2 watch * 2 reject * 2 time_stop = 8 combinations
        self.assertEqual(len(grid), 8)
        # 检查字段存在
        for p in grid:
            self.assertIn("watch_threshold", p)
            self.assertIn("reject_threshold", p)
            self.assertIn("time_stop_days", p)

    @patch("scripts.backtest.runner.get_strategy", side_effect=_mock_strategy)
    def test_apply_parameter_set_filters_by_veto(self, _mock):
        from scripts.backtest.runner import _apply_parameter_set, _baseline_parameters
        baseline = _baseline_parameters()
        trades = [
            {
                "code": "000001",
                "entry_date": "2026-04-01",
                "exit_date": "2026-04-05",
                "entry_price": 10.0,
                "exit_price": 11.0,
                "shares": 1000,
                "realized_pnl": 1000,
                "entry_score": 8.0,
                "veto_signals": ["below_ma20"],
            },
            {
                "code": "000002",
                "entry_date": "2026-04-01",
                "exit_date": "2026-04-05",
                "entry_price": 20.0,
                "exit_price": 22.0,
                "shares": 500,
                "realized_pnl": 1000,
                "entry_score": 7.5,
                "veto_signals": [],
            },
        ]
        # 启用 below_ma20 veto → 000001 被过滤
        params_with_veto = dict(baseline)
        params_with_veto["veto_rules"] = ["below_ma20"]
        result = _apply_parameter_set(trades, params_with_veto, baseline)
        codes = [t["code"] for t in result]
        self.assertNotIn("000001", codes)
        self.assertIn("000002", codes)

        # 禁用 veto → 两个都通过
        params_no_veto = dict(baseline)
        params_no_veto["veto_rules"] = []
        result2 = _apply_parameter_set(trades, params_no_veto, baseline)
        codes2 = [t["code"] for t in result2]
        self.assertIn("000001", codes2)
        self.assertIn("000002", codes2)

    @patch("scripts.backtest.runner.get_strategy", side_effect=_mock_strategy)
    def test_apply_parameter_set_filters_by_reject_threshold(self, _mock):
        from scripts.backtest.runner import _apply_parameter_set, _baseline_parameters
        baseline = _baseline_parameters()
        trades = [
            {
                "code": "000001",
                "entry_date": "2026-04-01",
                "exit_date": "2026-04-05",
                "entry_price": 10.0,
                "exit_price": 11.0,
                "shares": 1000,
                "realized_pnl": 1000,
                "entry_score": 4.0,  # 等于 reject_threshold
            },
            {
                "code": "000002",
                "entry_date": "2026-04-01",
                "exit_date": "2026-04-05",
                "entry_price": 20.0,
                "exit_price": 22.0,
                "shares": 500,
                "realized_pnl": 1000,
                "entry_score": 7.5,
            },
        ]
        params = dict(baseline)
        params["reject_threshold"] = 4.0
        params["buy_threshold"] = 3.0  # 低于 entry_score 以免被 buy_threshold 过滤
        result = _apply_parameter_set(trades, params, baseline)
        codes = [t["code"] for t in result]
        # entry_score 4.0 <= reject_threshold 4.0 → 被过滤
        self.assertNotIn("000001", codes)
        self.assertIn("000002", codes)


if __name__ == "__main__":
    unittest.main()
