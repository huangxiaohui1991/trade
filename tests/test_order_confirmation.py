import os
import tempfile
import unittest
from unittest import mock


class OrderConfirmationTests(unittest.TestCase):
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

    def test_apply_order_reply_marks_condition_order_confirmed(self):
        from scripts.state import apply_order_reply, load_order_snapshot, upsert_order_state

        self._bootstrap_empty()
        upsert_order_state(
            {
                "external_id": "paper:order:001",
                "scope": "paper_mx",
                "code": "300389",
                "name": "艾比森",
                "side": "sell",
                "order_class": "condition",
                "order_type": "conditional",
                "condition_type": "manual_stop",
                "requested_shares": 1000,
                "status": "candidate",
                "confirm_status": "pending",
                "trigger_price": 20.5,
                "updated_at": "2026-04-09T10:00:00",
            }
        )

        result = apply_order_reply("止损挂了 艾比森 ¥20.30")

        self.assertEqual(result["status"], "ok")
        self.assertFalse(result["created_order"])
        self.assertEqual(result["order"]["status"], "placed")
        self.assertEqual(result["order"]["confirm_status"], "confirmed")
        self.assertEqual(result["order"]["trigger_price"], 20.3)

        snapshot = load_order_snapshot(scope="paper_mx")
        self.assertEqual(snapshot["summary"]["status_counts"]["placed"], 1)

    def test_apply_order_reply_records_trade_event_on_fill(self):
        from scripts.state import apply_order_reply, load_activity_summary, upsert_order_state

        self._bootstrap_empty()
        upsert_order_state(
            {
                "external_id": "paper:order:002",
                "scope": "paper_mx",
                "code": "300389",
                "name": "艾比森",
                "side": "sell",
                "order_class": "condition",
                "order_type": "conditional",
                "condition_type": "take_profit_t1",
                "requested_shares": 600,
                "status": "placed",
                "confirm_status": "pending",
                "updated_at": "2026-04-09T10:00:00",
            }
        )

        result = apply_order_reply("止盈触发了 艾比森 成交¥21.30")

        self.assertEqual(result["status"], "ok")
        self.assertTrue(result["trade_event_recorded"])
        self.assertEqual(result["order"]["status"], "filled")
        self.assertEqual(result["order"]["avg_fill_price"], 21.3)

        activity = load_activity_summary(30, scope="paper_mx")
        self.assertEqual(activity["sell_count"], 1)
        self.assertEqual(activity["trade_count"], 1)

    def test_pending_condition_order_items_filters_terminal_orders(self):
        from scripts.state import pending_condition_order_items, upsert_order_state

        self._bootstrap_empty()
        upsert_order_state(
            {
                "external_id": "paper:order:003",
                "scope": "paper_mx",
                "code": "300389",
                "name": "艾比森",
                "side": "sell",
                "order_class": "condition",
                "order_type": "conditional",
                "condition_type": "manual_stop",
                "status": "candidate",
                "confirm_status": "pending",
                "trigger_price": 20.3,
                "updated_at": "2026-04-09T10:00:00",
            }
        )
        upsert_order_state(
            {
                "external_id": "paper:order:004",
                "scope": "paper_mx",
                "code": "603063",
                "name": "禾望电气",
                "side": "sell",
                "order_class": "condition",
                "order_type": "conditional",
                "condition_type": "take_profit_t1",
                "status": "filled",
                "confirm_status": "confirmed",
                "trigger_price": 33.8,
                "updated_at": "2026-04-09T10:01:00",
            }
        )

        items = pending_condition_order_items(scope="paper_mx")

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["name"], "艾比森")
        self.assertEqual(items[0]["type"], "止损")

    def test_apply_order_reply_supports_partial_fill(self):
        from scripts.state import apply_order_reply, load_activity_summary, load_order_snapshot, upsert_order_state

        self._bootstrap_empty()
        upsert_order_state(
            {
                "external_id": "paper:order:005",
                "scope": "paper_mx",
                "code": "300389",
                "name": "艾比森",
                "side": "sell",
                "order_class": "condition",
                "order_type": "conditional",
                "condition_type": "take_profit_t1",
                "requested_shares": 1000,
                "filled_shares": 0,
                "status": "placed",
                "confirm_status": "pending",
                "updated_at": "2026-04-09T10:00:00",
            }
        )

        result = apply_order_reply("止盈部分成交了 艾比森 300股 成交¥21.30")

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["order"]["status"], "partially_filled")
        self.assertEqual(result["order"]["filled_shares"], 300)
        self.assertEqual(result["order"]["avg_fill_price"], 21.3)

        snapshot = load_order_snapshot(scope="paper_mx")
        self.assertEqual(snapshot["summary"]["partial_fill_count"], 1)

        activity = load_activity_summary(30, scope="paper_mx")
        self.assertEqual(activity["sell_count"], 1)

    def test_apply_order_reply_supports_replace_and_review_queue(self):
        from scripts.state import apply_order_reply, load_order_snapshot, pending_condition_order_items, upsert_order_state

        self._bootstrap_empty()
        upsert_order_state(
            {
                "external_id": "paper:order:006",
                "scope": "paper_mx",
                "code": "300389",
                "name": "艾比森",
                "side": "sell",
                "order_class": "condition",
                "order_type": "conditional",
                "condition_type": "manual_stop",
                "requested_shares": 1000,
                "status": "placed",
                "confirm_status": "pending",
                "trigger_price": 20.5,
                "updated_at": "2026-04-09T10:00:00",
            }
        )

        replace_result = apply_order_reply("改挂止损 艾比森 ¥20.10")
        self.assertEqual(replace_result["order"]["status"], "cancel_replace_pending")
        self.assertEqual(replace_result["order"]["trigger_price"], 20.1)

        review_result = apply_order_reply("复核止损 艾比森")
        self.assertEqual(review_result["order"]["status"], "review_required")
        self.assertEqual(review_result["order"]["confirm_status"], "review_pending")

        snapshot = load_order_snapshot(scope="paper_mx")
        self.assertEqual(snapshot["summary"]["review_queue_count"], 1)
        self.assertEqual(snapshot["summary"]["cancel_replace_count"], 0)

        items = pending_condition_order_items(scope="paper_mx")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["status"], "review_pending")


if __name__ == "__main__":
    unittest.main()
