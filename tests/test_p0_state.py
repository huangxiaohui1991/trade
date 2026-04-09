import os
import tempfile
import unittest
from unittest import mock


class P0StateTests(unittest.TestCase):
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

    def test_bootstrap_primary_scope_excludes_hk_legacy(self):
        from scripts.state import bootstrap_state, load_activity_summary, load_portfolio_snapshot

        result = bootstrap_state(force=True)
        self.assertEqual(result["status"], "success")

        cn_snapshot = load_portfolio_snapshot(scope="cn_a_system")
        self.assertEqual(cn_snapshot["summary"]["holding_count"], 0)
        self.assertEqual(cn_snapshot["summary"]["current_exposure"], 0.0)

        hk_snapshot = load_portfolio_snapshot(scope="hk_legacy")
        self.assertGreaterEqual(len(hk_snapshot["positions"]), 1)

        weekly_activity = load_activity_summary("week", scope="cn_a_system")
        self.assertEqual(weekly_activity["weekly_buy_count"], 0)
        self.assertEqual(weekly_activity["buy_count"], 0)

    def test_warning_signal_is_not_hard_veto(self):
        from scripts.engine.scorer import get_recommendation, split_veto_signals

        hard_veto, warnings = split_veto_signals(["consecutive_outflow_warn"])
        self.assertEqual(hard_veto, [])
        self.assertEqual(warnings, ["consecutive_outflow_warn"])

        recommendation = get_recommendation({
            "total_score": 7.2,
            "veto_triggered": False,
            "veto_signals": ["consecutive_outflow_warn"],
        })
        self.assertNotIn("一票否决", recommendation)
        self.assertIn("流出预警", recommendation)

    def test_sync_portfolio_state_refreshes_structured_snapshot(self):
        from scripts.state import load_portfolio_snapshot, sync_portfolio_state

        result = sync_portfolio_state()
        self.assertEqual(result["status"], "success")

        snapshot = load_portfolio_snapshot(scope="cn_a_system")
        self.assertEqual(snapshot["summary"]["holding_count"], 0)
        self.assertEqual(snapshot["summary"]["current_exposure"], 0.0)

    def test_sync_activity_state_imports_weekly_records_by_scope(self):
        from scripts.state import load_activity_summary, sync_activity_state

        result = sync_activity_state()
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["imported_events"], 3)

        primary_summary = load_activity_summary("week", scope="cn_a_system")
        self.assertEqual(primary_summary["trade_count"], 0)
        self.assertEqual(primary_summary["buy_count"], 0)

        secondary_summary = load_activity_summary("week", scope="hk_legacy")
        self.assertEqual(secondary_summary["sell_count"], 3)
        self.assertEqual(secondary_summary["trade_count"], 3)
        self.assertEqual(secondary_summary["realized_pnl"], -62452.0)

    def test_stale_market_snapshot_is_refreshed(self):
        from scripts.state import load_market_snapshot, save_market_snapshot

        save_market_snapshot({
            "as_of_date": "2026-04-08",
            "updated_at": "2026-04-08T09:30:00",
            "signal": "RED",
            "source": "stale_test",
            "source_chain": ["stale_test"],
            "indices": {},
        })

        fresh_snapshot = {
            "as_of_date": "2026-04-09",
            "updated_at": "2026-04-09T09:30:00",
            "signal": "GREEN",
            "market_signal": "GREEN",
            "source": "market_timer_test",
            "source_chain": ["market_timer_test"],
            "indices": {
                "上证指数": {
                    "name": "上证指数",
                    "symbol": "sh000001",
                    "market_code": "000001",
                    "as_of_date": "2026-04-09",
                    "close": 3200,
                    "ma20": 3180,
                    "ma60": 3150,
                    "ma20_pct": 0.6,
                    "ma60_pct": 1.6,
                    "above_ma20": True,
                    "below_ma60_days": 0,
                    "signal": "GREEN",
                    "source": "market_timer_test",
                    "source_chain": ["market_timer_test"],
                }
            },
        }

        with mock.patch("scripts.engine.market_timer.load_market_snapshot", return_value=fresh_snapshot):
            snapshot = load_market_snapshot()

        self.assertEqual(snapshot["signal"], "GREEN")
        self.assertEqual(snapshot["as_of_date"], "2026-04-09")
        self.assertEqual(snapshot["source"], "market_timer_test")


if __name__ == "__main__":
    unittest.main()
