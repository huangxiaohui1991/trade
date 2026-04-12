import sys
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest import mock


class PipelineMarketHistoryTests(unittest.TestCase):
    def test_morning_archives_market_snapshot_history(self):
        import scripts.pipeline.morning as morning

        market_data = {
            "signal": "GREEN",
            "indices": {
                "上证指数": {
                    "close": 3200.0,
                    "change_pct": 1.2,
                    "ma20_pct": 0.5,
                    "ma60_pct": 1.0,
                    "signal": "GREEN",
                }
            },
        }
        fake_shadow_trade = SimpleNamespace(check_stop_signals=lambda dry_run=True: [])

        with mock.patch.dict(sys.modules, {"scripts.pipeline.shadow_trade": fake_shadow_trade}), mock.patch.object(morning, "ObsidianVault", return_value=mock.Mock()), mock.patch.object(
            morning, "DataEngine", return_value=mock.Mock()
        ), mock.patch.object(morning, "get_strategy", return_value={}), mock.patch.object(
            morning, "load_market_snapshot", return_value=market_data
        ), mock.patch.object(
            morning, "_get_portfolio_positions", return_value=[]
        ), mock.patch.object(
            morning, "_get_core_pool_status", return_value=[]
        ), mock.patch.object(
            morning, "_get_morning_news", return_value=[]
        ), mock.patch.object(
            morning, "_get_weekly_buy_count", return_value=1
        ), mock.patch.object(
            morning, "_build_discord_data", return_value={"market": {}, "positions": [], "core_pool": []}
        ), mock.patch.object(
            morning, "send_morning_summary", return_value=(True, None)
        ), mock.patch.object(
            morning, "save_market_snapshot_history"
        ) as save_history, mock.patch.object(
            morning, "update_pipeline_state"
        ) as update_state:
            result = morning.run()

        self.assertTrue(result["market_history_group_id"].startswith("morning:"))
        self.assertIn(":preopen:", result["market_history_group_id"])
        save_history.assert_called_once()
        self.assertEqual(save_history.call_args.kwargs["pipeline"], "morning")
        self.assertEqual(save_history.call_args.kwargs["metadata"]["timepoint"], "preopen")
        self.assertEqual(update_state.call_args.args[2]["timepoint"], "preopen")

    def test_noon_archives_market_snapshot_history(self):
        import scripts.pipeline.noon as noon

        market_snapshot = {
            "signal": "YELLOW",
            "indices": {
                "上证指数": {
                    "close": 3210.0,
                    "change_pct": 0.3,
                    "high": 3220.0,
                    "low": 3198.0,
                    "signal": "YELLOW",
                }
            },
        }
        fake_shadow_trade = SimpleNamespace(check_stop_signals=lambda: [])

        with mock.patch.dict(sys.modules, {"scripts.pipeline.shadow_trade": fake_shadow_trade}), mock.patch.object(noon, "DataEngine", return_value=mock.Mock()), mock.patch.object(
            noon, "load_market_snapshot", return_value=market_snapshot
        ), mock.patch.object(
            noon, "load_portfolio_snapshot", return_value={"positions": []}
        ), mock.patch.object(
            noon, "send_noon_check", return_value=(True, None)
        ), mock.patch.object(
            noon, "save_market_snapshot_history"
        ) as save_history, mock.patch.object(
            noon, "update_pipeline_state"
        ) as update_state:
            result = noon.run()

        self.assertTrue(result["market_history_group_id"].startswith("noon:"))
        self.assertIn(":midday:", result["market_history_group_id"])
        save_history.assert_called_once()
        self.assertEqual(save_history.call_args.kwargs["pipeline"], "noon")
        self.assertEqual(save_history.call_args.kwargs["metadata"]["timepoint"], "midday")
        self.assertEqual(update_state.call_args.args[2]["timepoint"], "midday")

    def test_evening_archives_market_snapshot_history(self):
        import scripts.pipeline.evening as evening

        market_data = {
            "signal": "RED",
            "indices": {
                "上证指数": {
                    "close": 3180.0,
                    "change_pct": -0.8,
                    "signal": "RED",
                }
            },
        }
        fake_shadow_trade = SimpleNamespace(
            check_stop_signals=lambda: [],
            generate_report=lambda: "/tmp/shadow_report.md",
        )

        with mock.patch.dict(sys.modules, {"scripts.pipeline.shadow_trade": fake_shadow_trade}), mock.patch.object(
            evening, "ObsidianVault", return_value=mock.Mock()
        ), mock.patch.object(
            evening, "DataEngine", return_value=mock.Mock()
        ), mock.patch.object(
            evening, "get_strategy", return_value={"risk": {"position": {"weekly_max": 2}}}
        ), mock.patch.object(
            evening, "load_market_snapshot", return_value=market_data
        ), mock.patch.object(
            evening, "_update_portfolio_prices", return_value=[]
        ), mock.patch.object(
            evening, "_backfill_today_trades", return_value=0
        ), mock.patch.object(
            evening, "_next_trading_day", return_value=datetime(2026, 4, 14)
        ), mock.patch.object(
            evening, "_generate_tomorrow_plan", return_value="PLAN"
        ), mock.patch.object(
            evening, "_create_tomorrow_journal"
        ), mock.patch.object(
            evening, "load_activity_summary", return_value={"weekly_buy_count": 1}
        ), mock.patch.object(
            evening, "load_pool_snapshot", return_value={"core_pool": []}
        ), mock.patch.object(
            evening, "send_evening_report", return_value=(True, None)
        ), mock.patch.object(
            evening, "_enrich_today_journal"
        ), mock.patch.object(
            evening, "save_market_snapshot_history"
        ) as save_history, mock.patch.object(
            evening, "update_pipeline_state"
        ) as update_state:
            result = evening.run()

        self.assertTrue(result["market_history_group_id"].startswith("evening:"))
        self.assertIn(":close:", result["market_history_group_id"])
        self.assertEqual(result["tomorrow_date"], "2026-04-14")
        save_history.assert_called_once()
        self.assertEqual(save_history.call_args.kwargs["pipeline"], "evening")
        self.assertEqual(save_history.call_args.kwargs["metadata"]["timepoint"], "close")
        self.assertEqual(update_state.call_args.args[2]["timepoint"], "close")


if __name__ == "__main__":
    unittest.main()
