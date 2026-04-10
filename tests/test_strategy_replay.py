"""Tests for the signal-driven strategy replay engine."""

import unittest
from scripts.backtest.strategy_replay import run_strategy_replay


class StrategyReplayTests(unittest.TestCase):

    def _make_daily_data(self):
        """构造 5 天的模拟数据。"""
        return {
            "2026-04-01": {
                "market_signal": "GREEN",
                "candidates": [
                    {
                        "code": "000001",
                        "name": "平安银行",
                        "score": 8.0,
                        "price": 10.0,
                        "veto_signals": [],
                        "technical_score": 1.5,
                        "fundamental_score": 2.5,
                        "flow_score": 1.5,
                        "sentiment_score": 2.5,
                    },
                    {
                        "code": "000002",
                        "name": "万科A",
                        "score": 7.5,
                        "price": 20.0,
                        "veto_signals": [],
                    },
                ],
                "prices": {"000001": 10.0, "000002": 20.0},
            },
            "2026-04-02": {
                "market_signal": "GREEN",
                "candidates": [],
                "prices": {"000001": 10.5, "000002": 19.5},
            },
            "2026-04-03": {
                "market_signal": "GREEN",
                "candidates": [],
                "prices": {"000001": 9.5, "000002": 21.0},
            },
            "2026-04-04": {
                "market_signal": "GREEN",
                "candidates": [],
                "prices": {"000001": 9.0, "000002": 24.0},
            },
            "2026-04-05": {
                "market_signal": "GREEN",
                "candidates": [],
                "prices": {"000001": 11.0, "000002": 22.0},
            },
        }

    def test_basic_entry_and_exit(self):
        """基本买入和止盈/止损触发。"""
        daily = self._make_daily_data()
        result = run_strategy_replay(
            daily,
            start="2026-04-01",
            end="2026-04-05",
            total_capital=100000,
            params={
                "buy_threshold": 7,
                "stop_loss": 0.08,
                "take_profit": 0.15,
                "time_stop_days": 30,
                "total_max": 0.60,
                "single_max": 0.20,
                "weekly_max": 5,
                "veto_rules": [],
            },
        )
        summary = result["summary"]
        self.assertEqual(summary["simulation_mode"], "signal_driven_strategy_replay")
        self.assertEqual(summary["capital"], 100000)
        self.assertGreater(summary["timeline_days"], 0)
        # 应该有入场记录
        self.assertGreater(len(result["timeline"][0]["entries"]), 0)

    def test_veto_blocks_entry(self):
        """veto 规则阻止买入。"""
        daily = {
            "2026-04-01": {
                "market_signal": "GREEN",
                "candidates": [
                    {
                        "code": "000001",
                        "name": "测试股",
                        "score": 8.0,
                        "price": 10.0,
                        "veto_signals": ["below_ma20"],
                    },
                ],
                "prices": {"000001": 10.0},
            },
        }
        result = run_strategy_replay(
            daily,
            start="2026-04-01",
            end="2026-04-01",
            total_capital=100000,
            params={
                "buy_threshold": 7,
                "veto_rules": ["below_ma20", "limit_up_today"],
            },
        )
        # 被 veto 拦截，不应有入场
        self.assertEqual(len(result["timeline"][0]["entries"]), 0)
        self.assertEqual(result["summary"]["veto_rejected_count"], 1)

    def test_veto_disabled_allows_entry(self):
        """禁用 veto 规则后允许买入。"""
        daily = {
            "2026-04-01": {
                "market_signal": "GREEN",
                "candidates": [
                    {
                        "code": "000001",
                        "name": "测试股",
                        "score": 8.0,
                        "price": 10.0,
                        "veto_signals": ["below_ma20"],
                    },
                ],
                "prices": {"000001": 10.0},
            },
        }
        result = run_strategy_replay(
            daily,
            start="2026-04-01",
            end="2026-04-01",
            total_capital=100000,
            params={
                "buy_threshold": 7,
                "veto_rules": [],  # 禁用所有 veto
            },
        )
        # veto 被禁用，应该入场
        self.assertEqual(len(result["timeline"][0]["entries"]), 1)
        self.assertEqual(result["summary"]["veto_rejected_count"], 0)

    def test_red_market_forces_exit(self):
        """RED 大盘信号强制平仓。"""
        daily = {
            "2026-04-01": {
                "market_signal": "GREEN",
                "candidates": [
                    {"code": "000001", "name": "测试", "score": 8.0, "price": 10.0, "veto_signals": []},
                ],
                "prices": {"000001": 10.0},
            },
            "2026-04-02": {
                "market_signal": "RED",
                "candidates": [],
                "prices": {"000001": 9.8},
            },
        }
        result = run_strategy_replay(
            daily,
            start="2026-04-01",
            end="2026-04-02",
            total_capital=100000,
            params={"buy_threshold": 7, "veto_rules": [], "weekly_max": 5},
        )
        # 第二天 RED 信号应该触发平仓
        self.assertGreater(len(result["closed_trades"]), 0)
        self.assertEqual(result["closed_trades"][0]["exit_reason"], "market_signal_exit")

    def test_stop_loss_triggers(self):
        """止损触发。"""
        daily = {
            "2026-04-01": {
                "market_signal": "GREEN",
                "candidates": [
                    {"code": "000001", "name": "测试", "score": 8.0, "price": 10.0, "veto_signals": []},
                ],
                "prices": {"000001": 10.0},
            },
            "2026-04-02": {
                "market_signal": "GREEN",
                "candidates": [],
                "prices": {"000001": 9.5},  # 跌 5%，超过 4% 止损
            },
        }
        result = run_strategy_replay(
            daily,
            start="2026-04-01",
            end="2026-04-02",
            total_capital=100000,
            params={"buy_threshold": 7, "stop_loss": 0.04, "veto_rules": [], "weekly_max": 5},
        )
        self.assertGreater(len(result["closed_trades"]), 0)
        self.assertEqual(result["closed_trades"][0]["exit_reason"], "stop_loss")

    def test_time_stop_triggers(self):
        """时间止损触发。"""
        daily = {}
        # 第一天买入
        daily["2026-04-01"] = {
            "market_signal": "GREEN",
            "candidates": [
                {"code": "000001", "name": "测试", "score": 8.0, "price": 10.0, "veto_signals": []},
            ],
            "prices": {"000001": 10.0},
        }
        # 后续 4 天价格不变（不触发止损止盈）
        for d in range(2, 6):
            daily[f"2026-04-{d:02d}"] = {
                "market_signal": "GREEN",
                "candidates": [],
                "prices": {"000001": 10.0},
            }
        result = run_strategy_replay(
            daily,
            start="2026-04-01",
            end="2026-04-05",
            total_capital=100000,
            params={
                "buy_threshold": 7,
                "stop_loss": 0.20,  # 宽止损不触发
                "take_profit": 0.50,  # 宽止盈不触发
                "time_stop_days": 3,  # 3 天时间止损
                "veto_rules": [],
                "weekly_max": 5,
            },
        )
        self.assertGreater(len(result["closed_trades"]), 0)
        self.assertEqual(result["closed_trades"][0]["exit_reason"], "time_stop")

    def test_weekly_max_limits_entries(self):
        """周买入次数限制。"""
        daily = {
            "2026-04-01": {
                "market_signal": "GREEN",
                "candidates": [
                    {"code": "000001", "name": "A", "score": 9.0, "price": 10.0, "veto_signals": []},
                    {"code": "000002", "name": "B", "score": 8.5, "price": 10.0, "veto_signals": []},
                    {"code": "000003", "name": "C", "score": 8.0, "price": 10.0, "veto_signals": []},
                ],
                "prices": {"000001": 10.0, "000002": 10.0, "000003": 10.0},
            },
        }
        result = run_strategy_replay(
            daily,
            start="2026-04-01",
            end="2026-04-01",
            total_capital=100000,
            params={
                "buy_threshold": 7,
                "weekly_max": 2,
                "veto_rules": [],
                "total_max": 0.90,
                "single_max": 0.30,
            },
        )
        # 最多买 2 只
        self.assertEqual(len(result["timeline"][0]["entries"]), 2)
        # 第三只被拒绝
        weekly_rejected = [r for r in result["rejected_entries"] if r["reason"] == "weekly_max_reached"]
        self.assertEqual(len(weekly_rejected), 1)

    def test_exposure_constraint(self):
        """总仓位上限约束。"""
        daily = {
            "2026-04-01": {
                "market_signal": "GREEN",
                "candidates": [
                    {"code": "000001", "name": "A", "score": 9.0, "price": 10.0, "veto_signals": []},
                    {"code": "000002", "name": "B", "score": 8.5, "price": 10.0, "veto_signals": []},
                ],
                "prices": {"000001": 10.0, "000002": 10.0},
            },
        }
        result = run_strategy_replay(
            daily,
            start="2026-04-01",
            end="2026-04-01",
            total_capital=100000,
            params={
                "buy_threshold": 7,
                "total_max": 0.15,  # 只允许 15% 仓位
                "single_max": 0.15,
                "weekly_max": 5,
                "veto_rules": [],
            },
        )
        # 总仓位 15000，只够买一只
        self.assertEqual(len(result["timeline"][0]["entries"]), 1)

    def test_output_structure_compatible(self):
        """输出结构与 _build_portfolio_replay 兼容。"""
        daily = self._make_daily_data()
        result = run_strategy_replay(
            daily,
            start="2026-04-01",
            end="2026-04-05",
            total_capital=100000,
            params={"buy_threshold": 7, "veto_rules": [], "weekly_max": 5},
        )
        # 检查必要字段
        self.assertIn("summary", result)
        self.assertIn("timeline", result)
        self.assertIn("closed_trades", result)
        self.assertIn("open_positions", result)
        summary = result["summary"]
        for key in [
            "capital", "total_exposure_max", "single_position_max",
            "timeline_days", "max_concurrent_positions", "peak_exposure_pct",
            "ending_realized_pnl", "ending_cash", "simulation_mode",
        ]:
            self.assertIn(key, summary, f"missing key: {key}")

    def test_cooldown_blocks_entry(self):
        """连续亏损冷却阻止新开仓。"""
        daily = {
            "2026-04-01": {
                "market_signal": "GREEN",
                "candidates": [
                    {"code": "000001", "name": "A", "score": 8.0, "price": 10.0, "veto_signals": []},
                ],
                "prices": {"000001": 10.0},
            },
            "2026-04-02": {
                "market_signal": "GREEN",
                "candidates": [],
                "prices": {"000001": 9.0},  # 触发止损
            },
            "2026-04-03": {
                "market_signal": "GREEN",
                "candidates": [
                    {"code": "000002", "name": "B", "score": 8.0, "price": 15.0, "veto_signals": []},
                ],
                "prices": {"000002": 15.0},
            },
        }
        result = run_strategy_replay(
            daily,
            start="2026-04-01",
            end="2026-04-03",
            total_capital=100000,
            params={
                "buy_threshold": 7,
                "stop_loss": 0.05,
                "veto_rules": [],
                "weekly_max": 5,
                "consecutive_loss_days_limit": 1,
                "cooldown_days": 2,
            },
        )
        # 第二天止损后进入冷却，第三天不应买入
        self.assertGreater(result["summary"]["cooldown_rejected_count"], 0)


if __name__ == "__main__":
    unittest.main()
