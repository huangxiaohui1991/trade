import os
import tempfile
import unittest
from unittest import mock


class SignalBusTests(unittest.TestCase):
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

    def test_reason_code_registry_normalizes_shared_aliases(self):
        from scripts.state.reason_codes import normalize_reason_code, reason_meta

        self.assertEqual(normalize_reason_code("consecutive_outflow_warn"), "POOL_WARNING_CONSECUTIVE_OUTFLOW")
        self.assertEqual(normalize_reason_code("market_signal=GREEN"), "MARKET_GREEN")
        self.assertEqual(reason_meta("RISK_TIME_STOP")["category"], "trade")
        self.assertEqual(reason_meta("RISK_TIME_STOP")["label"], "时间止损提示")

    def test_status_today_exposes_standard_signal_bus(self):
        from scripts.cli.trade import status_today

        with mock.patch("scripts.cli.trade.load_daily_state", return_value={
            "date": "2026-04-09",
            "updated_at": "2026-04-09T09:30:00",
            "pipelines": {},
        }), mock.patch("scripts.cli.trade.get_strategy", return_value={}), mock.patch(
            "scripts.cli.trade.build_today_decision",
            return_value={
                "decision": "NO_TRADE",
                "market_signal": "GREEN",
                "market_multiplier": 1.0,
                "current_exposure": 0.0,
                "weekly_buys": 1,
                "holding_count": 0,
                "positions_summary": {"holding_count": 0, "current_exposure": 0.0},
                "risk": {"can_buy": False},
                "reasons": ["market_signal=GREEN", "本周买入次数已满 (1/1)"],
            },
        ), mock.patch("scripts.cli.trade.load_portfolio_snapshot", return_value={
            "summary": {"holding_count": 0, "current_exposure": 0.0},
        }), mock.patch("scripts.cli.trade.load_pool_snapshot", return_value={
            "snapshot_date": "2026-04-09",
            "updated_at": "2026-04-09T09:30:00",
            "summary": {"core_count": 1, "watch_count": 0, "other_count": 0},
            "entries": [
                {
                    "code": "300389",
                    "name": "艾比森",
                    "bucket": "core",
                    "veto_triggered": False,
                    "veto_signals": ["consecutive_outflow_warn"],
                }
            ],
        }), mock.patch("scripts.cli.trade.audit_state", return_value={
            "status": "ok",
            "snapshot_date": "2026-04-09",
            "checks": {"stocks_yaml": {"ok": True}},
        }), mock.patch("scripts.cli.trade.load_market_snapshot", return_value={
            "signal": "GREEN",
            "source": "market_timer",
            "source_chain": ["market_timer"],
            "as_of_date": "2026-04-09",
        }), mock.patch("scripts.cli.trade._shadow_trade_snapshot", return_value={
            "status": "drift",
            "consistency": {
                "ok": False,
                "status": "drift",
                "event_only_codes": ["300389"],
                "broker_only_codes": [],
            },
            "advisory_summary": {
                "triggered_signal_count": 2,
                "triggered_position_count": 1,
                "triggered_rules": ["RISK_TIME_STOP"],
                "positions": [],
            },
        }):
            result = status_today(sync_state=False)

        self.assertIn("signal_bus", result)
        signal_bus = result["signal_bus"]
        self.assertEqual(signal_bus["version"], 1)
        self.assertEqual(signal_bus["market"]["primary_code"], "MARKET_GREEN")
        self.assertEqual(signal_bus["market"]["state"], "GREEN")
        self.assertIn("POOL_WARNING_CONSECUTIVE_OUTFLOW", signal_bus["pool"]["reason_codes"])
        self.assertIn("POOL_WARNING", signal_bus["pool"]["reason_codes"])
        self.assertEqual(signal_bus["pool"]["state"], "warning")
        self.assertIn("TRADE_WEEKLY_BUY_LIMIT", signal_bus["trade"]["reason_codes"])
        self.assertIn("TRADE_PAPER_RECONCILE_DRIFT", signal_bus["trade"]["reason_codes"])
        self.assertIn("RISK_TIME_STOP", signal_bus["trade"]["reason_codes"])
        self.assertEqual(signal_bus["trade"]["state"], "drift")


if __name__ == "__main__":
    unittest.main()
