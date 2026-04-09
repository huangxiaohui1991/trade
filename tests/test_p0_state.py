import os
import tempfile
import unittest


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


if __name__ == "__main__":
    unittest.main()
