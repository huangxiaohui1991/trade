import os
import tempfile
import unittest


class AlertCenterTests(unittest.TestCase):
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

    def _sample_context(self) -> dict:
        return {
            "today_decision": {
                "decision": "NO_TRADE",
                "market_signal": "CLEAR",
                "portfolio_risk": {
                    "state": "block",
                    "reason_codes": ["TRADE_CONSECUTIVE_LOSS_COOLDOWN"],
                    "reasons": ["连续亏损冷却中"],
                },
            },
            "pool_sync_state": {
                "status": "drift",
                "snapshot_date": "2026-04-09",
            },
            "shadow_snapshot": {
                "status": "drift",
                "consistency": {
                    "ok": False,
                    "status": "drift",
                    "event_only_codes": ["300389"],
                    "broker_only_codes": [],
                },
                "advisory_summary": {
                    "triggered_signal_count": 1,
                    "triggered_position_count": 1,
                    "triggered_rules": ["RISK_TIME_STOP"],
                },
            },
            "order_snapshot": {
                "scope": "paper_mx",
                "status": "all",
                "summary": {
                    "order_count": 2,
                    "pending_count": 1,
                    "open_count": 0,
                    "exception_count": 0,
                },
                "condition_orders": {
                    "count": 1,
                    "pending_count": 1,
                    "open_count": 0,
                    "exception_count": 0,
                    "status_counts": {"candidate": 1},
                    "condition_type_counts": {"manual_stop": 1},
                    "sample": [
                        {
                            "external_id": "paper:order:001",
                            "code": "300389",
                            "name": "艾比森",
                            "side": "sell",
                            "status": "candidate",
                            "condition_type": "manual_stop",
                            "requested_shares": 1000,
                            "filled_shares": 0,
                            "trigger_price": 20.5,
                            "limit_price": 20.45,
                            "confirm_status": "pending",
                        }
                    ],
                },
            },
            "signal_bus": {
                "version": 1,
                "state": "drift",
                "market": {
                    "primary_code": "MARKET_CLEAR",
                    "state": "CLEAR",
                },
                "pool": {
                    "primary_code": "POOL_SYNC_DRIFT",
                    "state": "drift",
                    "reason_codes": ["POOL_SYNC_DRIFT"],
                },
                "trade": {
                    "primary_code": "TRADE_PAPER_RECONCILE_DRIFT",
                    "state": "drift",
                    "reason_codes": ["TRADE_PAPER_RECONCILE_DRIFT"],
                },
            },
            "pool_snapshot": {
                "snapshot_date": "2026-04-09",
                "updated_at": "2026-04-09T09:30:00",
                "summary": {
                    "core_count": 1,
                    "watch_count": 1,
                    "other_count": 0,
                },
                "entries": [
                    {
                        "bucket": "core",
                        "code": "300389",
                        "name": "艾比森",
                        "total_score": 7.6,
                        "technical_score": 1.6,
                        "fundamental_score": 2.0,
                        "flow_score": 1.5,
                        "sentiment_score": 2.5,
                        "veto_triggered": False,
                        "veto_signals": [],
                        "note": "核心池",
                        "source": "unit_test",
                    },
                    {
                        "bucket": "watch",
                        "code": "603063",
                        "name": "禾望电气",
                        "total_score": 6.9,
                        "technical_score": 1.3,
                        "fundamental_score": 1.7,
                        "flow_score": 1.5,
                        "sentiment_score": 2.4,
                        "veto_triggered": False,
                        "veto_signals": ["consecutive_outflow_warn"],
                        "note": "预警",
                        "source": "unit_test",
                    },
                ],
            },
            "market_snapshot": {
                "signal": "CLEAR",
                "source": "market_timer",
                "source_chain": ["market_timer"],
                "as_of_date": "2026-04-09",
            },
        }

    def test_build_alert_center_snapshot_counts_and_ack(self):
        from scripts.state import build_alert_center_snapshot

        snapshot = build_alert_center_snapshot(**self._sample_context())

        self.assertEqual(snapshot["status"], "warning")
        self.assertEqual(snapshot["alert_count"], 6)
        self.assertEqual(snapshot["status_summary"]["alert_count"], 6)
        self.assertEqual(snapshot["status_summary"]["level_counts"]["warning"], 3)
        self.assertEqual(snapshot["status_summary"]["level_counts"]["info"], 3)
        self.assertEqual(snapshot["classification"]["by_code"]["POOL_SYNC_DRIFT"], 1)
        self.assertEqual(snapshot["classification"]["by_code"]["ORDER_CONFIRM_PENDING"], 1)
        self.assertNotIn("ORDER_EXCEPTION", snapshot["classification"]["by_code"])
        self.assertTrue(snapshot["status_summary"]["recent_updated_at"])
        self.assertEqual(snapshot["status_summary"]["ack_summary"]["acknowledged_count"], 0)
        self.assertEqual(snapshot["status_summary"]["ack_summary"]["pending_count"], 6)
        self.assertTrue(all(alert["acknowledged"] is False for alert in snapshot["alerts"]))
        self.assertTrue(all(alert["acknowledged_at"] == "" for alert in snapshot["alerts"]))
        self.assertTrue(all(alert["handling_status"] == "pending" for alert in snapshot["alerts"]))
        self.assertTrue(all(alert["alert_key"] for alert in snapshot["alerts"]))

    def test_pool_snapshot_alerts_cover_financial_market_and_score_loss(self):
        from scripts.state import build_alert_center_snapshot

        context = self._sample_context()
        context["pool_sync_state"] = {"status": "ok", "snapshot_date": "2026-04-09"}
        context["shadow_snapshot"] = {"status": "ok", "consistency": {"ok": True, "status": "ok"}, "advisory_summary": {}}
        context["order_snapshot"] = {"summary": {"pending_count": 0, "exception_count": 0}, "condition_orders": {}}
        context["today_decision"] = {"market_signal": "GREEN", "portfolio_risk": {"state": "ok"}}
        context["pool_snapshot"]["entries"] = [
            {
                "bucket": "core",
                "code": "300389",
                "name": "艾比森",
                "total_score": 6.1,
                "veto_signals": ["earnings_bomb", "limit_up_today", "volume_break"],
                "metadata": {"score_delta": -1.4},
            },
            {
                "bucket": "core",
                "code": "300389",
                "name": "艾比森",
                "total_score": 6.1,
                "veto_signals": ["earnings_bomb"],
                "metadata": {"score_delta": -1.4},
            },
        ]

        snapshot = build_alert_center_snapshot(**context)
        codes = snapshot["classification"]["by_code"]

        self.assertEqual(codes["FINANCIAL_EARNINGS_WARNING"], 1)
        self.assertEqual(codes["MARKET_LIMIT_UP_PULLBACK_WATCH"], 1)
        self.assertEqual(codes["MARKET_VOLUME_BREAK_WARNING"], 1)
        self.assertEqual(codes["POOL_SCORE_LOSS"], 1)
        self.assertEqual(snapshot["ack_summary"]["suppressed_duplicate_count"], 2)
        self.assertEqual(snapshot["alert_count"], 4)

    def test_load_alert_snapshot_persists_and_round_trips(self):
        from scripts.state import load_alert_snapshot

        first = load_alert_snapshot(context=self._sample_context())
        second = load_alert_snapshot(context=self._sample_context())

        self.assertEqual(first["status_summary"]["code_counts"], second["status_summary"]["code_counts"])
        self.assertEqual(first["classification"]["by_level"], second["classification"]["by_level"])
        self.assertEqual(second["snapshot_date"], "2026-04-09")
        self.assertEqual(second["status_summary"]["ack_summary"]["pending_count"], 6)
        self.assertTrue(all(alert["acknowledged"] is False for alert in second["alerts"]))


if __name__ == "__main__":
    unittest.main()
