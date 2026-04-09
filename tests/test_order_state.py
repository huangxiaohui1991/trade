import os
import sqlite3
import tempfile
import unittest


class OrderStateTests(unittest.TestCase):
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

    def test_upsert_order_state_preserves_trade_events_and_merges_existing_fields(self):
        from scripts.state import load_order_snapshot, upsert_order_state

        first = upsert_order_state(
            {
                "external_id": "mx:order:001",
                "scope": "paper_mx",
                "broker": "mx_moni",
                "broker_order_id": "BRK-001",
                "code": "300389",
                "name": "艾比森",
                "side": "buy",
                "order_class": "condition",
                "order_type": "conditional",
                "condition_type": "take_profit_t1",
                "requested_shares": 1000,
                "trigger_price": 20.5,
                "limit_price": 20.45,
                "status": "placed",
                "confirm_status": "pending",
                "reason_code": "RISK_TIME_STOP",
                "reason_text": "止损待确认",
                "source": "unit_test",
                "placed_at": "2026-04-09T10:00:00",
                "updated_at": "2026-04-09T10:01:00",
                "metadata": {"note": "first"},
            }
        )
        self.assertEqual(first["external_id"], "mx:order:001")
        self.assertEqual(first["status"], "placed")

        second = upsert_order_state(
            {
                "external_id": "mx:order:001",
                "scope": "paper_mx",
                "filled_shares": 400,
                "avg_fill_price": 20.4,
                "status": "partially_filled",
                "metadata": {"broker_fill": "partial"},
                "updated_at": "2026-04-09T10:02:00",
            }
        )
        self.assertEqual(second["status"], "partially_filled")
        self.assertEqual(second["filled_shares"], 400)

        snapshot = load_order_snapshot(scope="paper_mx")
        self.assertEqual(snapshot["summary"]["order_count"], 1)
        self.assertEqual(snapshot["summary"]["open_count"], 1)
        self.assertEqual(snapshot["summary"]["status_counts"]["partially_filled"], 1)

        order = snapshot["orders"][0]
        self.assertEqual(order["code"], "300389")
        self.assertEqual(order["name"], "艾比森")
        self.assertEqual(order["requested_shares"], 1000)
        self.assertEqual(order["filled_shares"], 400)
        self.assertEqual(order["avg_fill_price"], 20.4)
        self.assertEqual(order["metadata"], {"broker_fill": "partial", "note": "first"})

        with sqlite3.connect(os.environ["TRADE_STATE_DB_PATH"]) as conn:
            trade_events = conn.execute("SELECT COUNT(*) FROM trade_events").fetchone()[0]
        self.assertEqual(trade_events, 0)

    def test_load_order_snapshot_filters_by_scope_and_status(self):
        from scripts.state import load_order_snapshot, upsert_order_state

        upsert_order_state(
            {
                "external_id": "mx:order:001",
                "scope": "paper_mx",
                "code": "300389",
                "name": "艾比森",
                "status": "filled",
                "order_class": "risk",
                "order_type": "market",
                "filled_shares": 1000,
                "avg_fill_price": 20.5,
                "updated_at": "2026-04-09T10:10:00",
            }
        )
        upsert_order_state(
            {
                "external_id": "cn:order:002",
                "scope": "cn_a_system",
                "code": "603063",
                "name": "禾望电气",
                "status": "cancelled",
                "order_class": "manual",
                "order_type": "limit",
                "requested_shares": 600,
                "updated_at": "2026-04-09T10:11:00",
            }
        )

        all_snapshot = load_order_snapshot()
        self.assertEqual(all_snapshot["summary"]["order_count"], 2)
        self.assertEqual(all_snapshot["summary"]["terminal_count"], 2)
        self.assertEqual(all_snapshot["summary"]["open_count"], 0)
        self.assertEqual(all_snapshot["summary"]["scope_counts"]["paper_mx"], 1)
        self.assertEqual(all_snapshot["summary"]["scope_counts"]["cn_a_system"], 1)

        paper_snapshot = load_order_snapshot(scope="paper_mx")
        self.assertEqual(paper_snapshot["summary"]["order_count"], 1)
        self.assertEqual(paper_snapshot["summary"]["status_counts"]["filled"], 1)

        filled_snapshot = load_order_snapshot(status="filled")
        self.assertEqual(filled_snapshot["summary"]["order_count"], 1)
        self.assertEqual(filled_snapshot["orders"][0]["scope"], "paper_mx")

