import os
import tempfile
import unittest
from unittest import mock


class TradeReviewTests(unittest.TestCase):
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

    def _bootstrap_empty(self):
        from scripts.state.service import bootstrap_state

        with mock.patch(
            "scripts.state.service._bootstrap_portfolio_snapshot",
            return_value=([], [], "2026-04-09"),
        ), mock.patch(
            "scripts.state.service._bootstrap_trade_events",
            return_value=[],
        ), mock.patch(
            "scripts.state.service._bootstrap_pool_entries",
            return_value=[],
        ):
            bootstrap_state(force=True)

    def test_load_trade_review_builds_closed_trade_attribution(self):
        from scripts.state import load_trade_review, record_trade_event

        self._bootstrap_empty()
        record_trade_event(
            {
                "external_id": "t1",
                "scope": "cn_a_system",
                "code": "300389",
                "name": "艾比森",
                "side": "buy",
                "shares": 1000,
                "price": 19.1,
                "amount": 19100,
                "event_date": "2026-04-07",
                "reason_code": "BUY_CORE_POOL",
                "reason_text": "核心池评分7.6",
                "source": "unit_test",
            }
        )
        record_trade_event(
            {
                "external_id": "t2",
                "scope": "cn_a_system",
                "code": "300389",
                "name": "艾比森",
                "side": "sell",
                "shares": 1000,
                "price": 21.3,
                "amount": 21300,
                "realized_pnl": 2200,
                "event_date": "2026-04-09",
                "reason_code": "RISK_TAKE_PROFIT_T1",
                "reason_text": "第一批止盈",
                "source": "unit_test",
            }
        )

        review = load_trade_review(window=30, scope="cn_a_system")

        self.assertEqual(review["closed_trade_count"], 1)
        self.assertEqual(review["win_count"], 1)
        self.assertEqual(review["loss_count"], 0)
        trade = review["closed_trades"][0]
        self.assertEqual(trade["code"], "300389")
        self.assertEqual(trade["entry_reason_code"], "BUY_CORE_POOL")
        self.assertIn("RISK_TAKE_PROFIT_T1", trade["exit_reason_codes"])
        self.assertEqual(trade["holding_days"], 2)
        self.assertEqual(trade["realized_pnl"], 2200)
        self.assertIn("risk", trade["rule_tags"])
        self.assertIn("entry", trade["rule_tags"])
        self.assertIsNotNone(trade["mfe_pct"])
        self.assertIsNotNone(trade["mae_pct"])
        self.assertEqual(review["mfe_mae_status"], "proxy_market_history")

    def test_load_trade_review_adds_summary_and_weekly_section(self):
        from scripts.pipeline.weekly_review import _build_weekly_report
        from scripts.state import load_activity_summary, load_trade_review, record_trade_event

        self._bootstrap_empty()
        for event in [
            {
                "external_id": "t1",
                "scope": "cn_a_system",
                "code": "300389",
                "name": "艾比森",
                "side": "buy",
                "shares": 1000,
                "price": 19.1,
                "amount": 19100,
                "event_date": "2026-04-01",
                "reason_code": "BUY_CORE_POOL",
                "reason_text": "核心池评分7.6",
                "source": "unit_test",
            },
            {
                "external_id": "t2",
                "scope": "cn_a_system",
                "code": "300389",
                "name": "艾比森",
                "side": "sell",
                "shares": 1000,
                "price": 21.3,
                "amount": 21300,
                "realized_pnl": 2200,
                "event_date": "2026-04-03",
                "reason_code": "RISK_TAKE_PROFIT_T1",
                "reason_text": "第一批止盈",
                "source": "unit_test",
            },
            {
                "external_id": "t3",
                "scope": "cn_a_system",
                "code": "688001",
                "name": "华兴源创",
                "side": "buy",
                "shares": 500,
                "price": 28.0,
                "amount": 14000,
                "event_date": "2026-04-02",
                "reason_code": "BUY_CORE_POOL",
                "reason_text": "核心池评分7.2",
                "source": "unit_test",
            },
            {
                "external_id": "t4",
                "scope": "cn_a_system",
                "code": "688001",
                "name": "华兴源创",
                "side": "sell",
                "shares": 500,
                "price": 27.0,
                "amount": 13500,
                "realized_pnl": -500,
                "event_date": "2026-04-06",
                "reason_code": "PAPER_RECONCILE_FLATTEN",
                "reason_text": "补录缺失平仓",
                "source": "unit_test",
            },
        ]:
            record_trade_event(event)

        review = load_trade_review(window=30, scope="cn_a_system")
        summary = review["summary_stats"]
        self.assertEqual(review["closed_trade_count"], 2)
        self.assertEqual(summary["avg_holding_days"], 3.0)
        self.assertEqual(summary["avg_win"], 2200.0)
        self.assertEqual(summary["avg_loss"], -500.0)
        self.assertEqual(summary["rule_break_count"], 1)
        self.assertIsNotNone(summary["avg_mfe_pct"])
        self.assertIsNotNone(summary["avg_mae_pct"])

        trades = {item["code"]: item for item in review["closed_trades"]}
        self.assertEqual(trades["300389"]["holding_days_bucket"], "0-3天")
        self.assertEqual(trades["300389"]["exit_style"], "risk")
        self.assertEqual(trades["300389"]["rule_compliance"]["status"], "compliant")
        self.assertEqual(trades["300389"]["rule_compliance"]["rule_break_count"], 0)
        self.assertEqual(trades["688001"]["holding_days_bucket"], "4-7天")
        self.assertEqual(trades["688001"]["exit_style"], "manual")
        self.assertEqual(trades["688001"]["rule_compliance"]["status"], "reconcile")
        self.assertTrue(trades["688001"]["rule_compliance"]["has_reconcile"])
        self.assertEqual(trades["688001"]["rule_compliance"]["rule_break_count"], 1)

        vault = mock.Mock(vault_path=self._tmpdir.name, read_core_pool=mock.Mock(return_value=[]))
        activity = load_activity_summary(30, scope="cn_a_system")
        report, _ = _build_weekly_report(
            vault,
            activity,
            [],
            activity.get("trade_events", []),
            2026,
            15,
            [],
            review,
        )
        self.assertIn("## 复盘归因（结构化闭合交易）", report)
        self.assertIn("| 平均持有天数 | 3.0 天 |", report)
        self.assertIn("| 平均盈利单笔 | ¥+2,200.00 |", report)
        self.assertIn("| 平均亏损单笔 | ¥-500.00 |", report)
        self.assertIn("| 规则违例数 | 1 |", report)


if __name__ == "__main__":
    unittest.main()
