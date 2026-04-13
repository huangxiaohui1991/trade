import unittest
from unittest import mock


class DataQualityGateTests(unittest.TestCase):
    def test_recommendation_downgrades_degraded_buy_signal_to_manual_review(self):
        from scripts.engine.scorer import get_recommendation

        recommendation = get_recommendation({
            "total_score": 8.1,
            "veto_signals": [],
            "veto_triggered": False,
            "data_quality": "degraded",
            "data_missing_fields": ["营收", "现金流"],
        })

        self.assertIn("人工复核", recommendation)
        self.assertIn("数据降级", recommendation)
        self.assertNotIn("可买入", recommendation)

    def test_recommendation_blocks_error_quality(self):
        from scripts.engine.scorer import get_recommendation

        recommendation = get_recommendation({
            "total_score": 8.1,
            "veto_signals": [],
            "veto_triggered": False,
            "data_quality": "error",
        })

        self.assertIn("暂停买入", recommendation)
        self.assertNotIn("可买入", recommendation)

    def test_pool_actions_route_degraded_promotion_to_manual_review(self):
        from scripts.utils import pool_manager

        with (
            mock.patch.object(pool_manager, "_load_previous_code_state", return_value={}),
            mock.patch.object(pool_manager, "save_pool_snapshot", return_value="mock_db_path"),
        ):
            suggestions, meta = pool_manager.evaluate_pool_actions(
                [
                    {
                        "code": "300389",
                        "name": "艾比森",
                        "total_score": 8.1,
                        "veto_signals": [],
                        "veto_triggered": False,
                        "data_quality": "degraded",
                        "data_missing_fields": ["营收"],
                    },
                ],
                stocks_cfg={"core_pool": [], "watch_pool": [{"code": "300389", "name": "艾比森"}]},
                strategy_cfg={
                    "pool_management": {
                        "watch_min_score": 5,
                        "promote_min_score": 7,
                        "promote_streak_days": 1,
                        "demote_max_score": 5,
                        "demote_streak_days": 2,
                        "remove_max_score": 4,
                        "remove_streak_days": 2,
                        "add_to_watch_streak_days": 1,
                    },
                },
                current_snapshot={
                    "entries": [
                        {
                            "code": "300389",
                            "name": "艾比森",
                            "bucket": "watch",
                            "total_score": 6.5,
                        },
                    ]
                },
                source="unit_test",
            )

        self.assertEqual(suggestions["promote_to_core"], [])
        self.assertEqual(suggestions["manual_review"][0]["code"], "300389")
        self.assertIn("数据降级", suggestions["manual_review"][0]["reason"])

        snapshot_entry = next(item for item in meta["snapshot_entries"] if item["code"] == "300389")
        self.assertEqual(snapshot_entry["bucket"], "watch")
        self.assertIn("人工复核", snapshot_entry["note"])
        self.assertEqual(snapshot_entry["data_quality"], "degraded")

    def test_shadow_buy_blocks_non_ok_data_quality_before_price_or_order(self):
        from scripts.pipeline import shadow_trade

        for quality, status, reason_code in [
            ("degraded", "人工复核", "DATA_QUALITY_MANUAL_REVIEW"),
            ("error", "blocked", "DATA_QUALITY_BLOCKED"),
        ]:
            with self.subTest(quality=quality):
                with mock.patch.object(
                    shadow_trade,
                    "get_stocks",
                    return_value={"core_pool": [{"code": "300389", "name": "艾比森"}]},
                ), mock.patch.object(
                    shadow_trade,
                    "_get_positions",
                    return_value=[],
                ), mock.patch.object(
                    shadow_trade,
                    "_get_balance",
                    return_value={"available": 100000},
                ), mock.patch.object(
                    shadow_trade,
                    "get_strategy",
                    return_value={"scoring": {"thresholds": {"buy": 7}}},
                ), mock.patch.object(
                    shadow_trade,
                    "score_stock",
                    return_value={
                        "total_score": 8.1,
                        "veto_signals": [],
                        "veto_triggered": False,
                        "data_quality": quality,
                        "data_missing_fields": ["营收"],
                    },
                ), mock.patch.object(
                    shadow_trade,
                    "_query_mx",
                ) as query_mock, mock.patch.object(
                    shadow_trade,
                    "_submit_shadow_order",
                ) as order_mock:
                    results = shadow_trade.buy_new_picks()

                self.assertEqual(results[0]["status"], status)
                self.assertEqual(results[0]["reason_code"], reason_code)
                self.assertEqual(results[0]["data_quality"], quality)
                query_mock.assert_not_called()
                order_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
