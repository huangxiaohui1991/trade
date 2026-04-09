import os
import tempfile
import unittest
from unittest import mock


class ShadowOrderFlowTests(unittest.TestCase):
    def setUp(self):
        self._old_db_path = os.environ.get("TRADE_STATE_DB_PATH")
        self._old_vault = os.environ.get("AStockVault")
        self._tmpdir = tempfile.TemporaryDirectory()
        os.environ["TRADE_STATE_DB_PATH"] = os.path.join(self._tmpdir.name, "trade_state.sqlite3")
        os.environ["AStockVault"] = self._tmpdir.name

    def tearDown(self):
        if self._old_db_path is None:
            os.environ.pop("TRADE_STATE_DB_PATH", None)
        else:
            os.environ["TRADE_STATE_DB_PATH"] = self._old_db_path
        if self._old_vault is None:
            os.environ.pop("AStockVault", None)
        else:
            os.environ["AStockVault"] = self._old_vault
        self._tmpdir.cleanup()

    def _bootstrap_empty_ledger(self):
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

    def test_market_order_is_logged_immediately(self):
        from scripts.pipeline.shadow_trade import _submit_shadow_order
        from scripts.state import load_activity_summary, load_order_snapshot

        self._bootstrap_empty_ledger()
        mx = mock.Mock()
        mx.trade.return_value = {"code": "200", "orderId": "ORD-1"}

        _, order = _submit_shadow_order(
            mx,
            side="buy",
            code="300389",
            name="艾比森",
            shares=1000,
            reason="核心池评分8.1",
            reason_code="BUY_CORE_POOL",
            price=19.8,
            use_market_price=True,
            order_class="manual",
        )

        self.assertEqual(order["status"], "filled")
        snapshot = load_order_snapshot(scope="paper_mx")
        self.assertEqual(snapshot["summary"]["order_count"], 1)
        self.assertEqual(snapshot["summary"]["open_count"], 0)
        self.assertEqual(snapshot["summary"]["status_counts"]["filled"], 1)

        activity = load_activity_summary(30, scope="paper_mx")
        self.assertEqual(activity["buy_count"], 1)
        self.assertEqual(activity["trade_count"], 1)

    def test_limit_order_waits_for_broker_fill_before_trade_event(self):
        from scripts.pipeline.shadow_trade import _submit_shadow_order, _sync_broker_orders
        from scripts.state import load_activity_summary, load_order_snapshot

        self._bootstrap_empty_ledger()
        mx = mock.Mock()
        mx.trade.return_value = {"code": "200", "orderId": "ORD-2"}

        _, placed_order = _submit_shadow_order(
            mx,
            side="sell",
            code="300389",
            name="艾比森",
            shares=600,
            reason="止盈第一批 现价¥21.30 > ¥21.00 (+11.0%)",
            reason_code="RISK_TAKE_PROFIT_T1",
            price=21.3,
            use_market_price=False,
            order_class="risk",
        )

        self.assertEqual(placed_order["status"], "placed")
        before_activity = load_activity_summary(30, scope="paper_mx")
        self.assertEqual(before_activity["trade_count"], 0)

        broker = mock.Mock()
        broker.orders.return_value = {
            "data": {
                "orderList": [
                    {
                        "orderId": "ORD-2",
                        "stockCode": "300389",
                        "stockName": "艾比森",
                        "type": "sell",
                        "status": "已成交",
                        "quantity": 600,
                        "filledQuantity": 600,
                        "avgPrice": 21.3,
                    }
                ]
            }
        }

        result = _sync_broker_orders(broker)
        self.assertEqual(result["status"], "ok")

        snapshot = load_order_snapshot(scope="paper_mx")
        self.assertEqual(snapshot["summary"]["status_counts"]["filled"], 1)
        self.assertEqual(snapshot["summary"]["open_count"], 0)

        after_activity = load_activity_summary(30, scope="paper_mx")
        self.assertEqual(after_activity["sell_count"], 1)
        self.assertEqual(after_activity["trade_count"], 1)

        _sync_broker_orders(broker)
        repeated_activity = load_activity_summary(30, scope="paper_mx")
        self.assertEqual(repeated_activity["trade_count"], 1)


if __name__ == "__main__":
    unittest.main()
