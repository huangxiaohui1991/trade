import os
import tempfile
import unittest
from unittest import mock


class BacktestEngineTests(unittest.TestCase):
    def setUp(self):
        self._old_db_path = os.environ.get("TRADE_STATE_DB_PATH")
        self._tmpdir = tempfile.TemporaryDirectory()
        os.environ["TRADE_STATE_DB_PATH"] = os.path.join(self._tmpdir.name, "trade_state.sqlite3")

    def tearDown(self):
        if self._old_db_path is None:
            os.environ.pop("TRADE_STATE_DB_PATH", None)
        else:
            os.environ["TRADE_STATE_DB_PATH"] = self._old_db_path
        self._tmpdir.cleanup()

    def _seed_trade_rounds(self):
        from scripts.state import record_trade_event

        record_trade_event(
            {
                "external_id": "bt-001-buy",
                "scope": "cn_a_system",
                "code": "300389",
                "name": "艾比森",
                "side": "buy",
                "shares": 1000,
                "price": 20.0,
                "event_date": "2026-03-01",
                "reason_code": "BUY_SCORE_PASS",
            }
        )
        record_trade_event(
            {
                "external_id": "bt-001-sell",
                "scope": "cn_a_system",
                "code": "300389",
                "name": "艾比森",
                "side": "sell",
                "shares": 1000,
                "price": 22.0,
                "event_date": "2026-03-05",
                "realized_pnl": 2000,
                "reason_code": "RISK_TAKE_PROFIT_T1",
            }
        )
        record_trade_event(
            {
                "external_id": "bt-002-buy",
                "scope": "cn_a_system",
                "code": "603063",
                "name": "禾望电气",
                "side": "buy",
                "shares": 600,
                "price": 30.0,
                "event_date": "2026-03-15",
                "reason_code": "BUY_SCORE_PASS",
            }
        )
        record_trade_event(
            {
                "external_id": "bt-002-sell",
                "scope": "cn_a_system",
                "code": "603063",
                "name": "禾望电气",
                "side": "sell",
                "shares": 600,
                "price": 28.0,
                "event_date": "2026-03-20",
                "realized_pnl": -1200,
                "reason_code": "RISK_ABSOLUTE_STOP_LOSS",
            }
        )
        record_trade_event(
            {
                "external_id": "bt-003-buy",
                "scope": "cn_a_system",
                "code": "000612",
                "name": "焦作万方",
                "side": "buy",
                "shares": 800,
                "price": 12.0,
                "event_date": "2026-04-01",
                "reason_code": "BUY_SCORE_PASS",
            }
        )
        record_trade_event(
            {
                "external_id": "bt-003-sell",
                "scope": "cn_a_system",
                "code": "000612",
                "name": "焦作万方",
                "side": "sell",
                "shares": 800,
                "price": 12.5,
                "event_date": "2026-04-06",
                "realized_pnl": 400,
                "reason_code": "POOL_DEMOTE",
            }
        )

    def test_run_backtest_summarizes_closed_trades(self):
        from scripts.backtest import run_backtest

        self._seed_trade_rounds()

        with mock.patch("scripts.state.service._load_trade_history_rows", return_value=[]):
            result = run_backtest(start="2026-03-01", end="2026-04-09", scope="cn_a_system")

        self.assertEqual(result["command"], "backtest")
        self.assertEqual(result["action"], "run")
        self.assertEqual(result["sample_count"], 3)
        self.assertEqual(result["score_summary"]["win_count"], 2)
        self.assertEqual(result["score_summary"]["loss_count"], 1)
        self.assertEqual(result["score_summary"]["win_rate"], 66.7)
        self.assertEqual(result["score_summary"]["total_realized_pnl"], 1200.0)
        self.assertEqual(result["state_fields"]["trade"]["closed_trade_count"], 3)
        self.assertEqual(result["state_fields"]["decision"]["action"], "NO_TRADE")
        self.assertEqual(result["parameters"]["scope"], "cn_a_system")

    def test_run_walk_forward_builds_fold_windows(self):
        from scripts.backtest import run_walk_forward

        self._seed_trade_rounds()

        with mock.patch("scripts.state.service._load_trade_history_rows", return_value=[]):
            result = run_walk_forward(start="2026-03-01", end="2026-04-09", scope="cn_a_system", folds=3)

        self.assertEqual(result["command"], "backtest")
        self.assertEqual(result["action"], "walk-forward")
        self.assertGreaterEqual(result["sample_count"], 1)
        self.assertEqual(result["parameters"]["folds"], 3)
        self.assertEqual(len(result["folds"]), 3)
        self.assertIn("training_summary", result["folds"][0]["score_summary"])
        self.assertIn("evaluation_summary", result["folds"][0]["score_summary"])
        self.assertIn("selected_parameters", result["folds"][0]["score_summary"])
        self.assertEqual(result["score_summary"]["fold_count"], 3)
        self.assertEqual(result["risk_summary"]["fold_count"], 3)
        self.assertIn(result["status"], {"ok", "warning", "drift"})
