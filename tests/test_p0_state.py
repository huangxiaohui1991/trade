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

    def test_shadow_position_context_resets_after_flat(self):
        from scripts.pipeline.shadow_trade import _build_open_position_context

        context = _build_open_position_context([
            {"event_date": "2026-03-01", "side": "buy", "code": "300389", "shares": 1000, "price": 19.13},
            {"event_date": "2026-03-05", "side": "sell", "code": "300389", "shares": 1000, "price": 19.50},
            {"event_date": "2026-03-20", "side": "buy", "code": "300389", "shares": 500, "price": 18.80},
        ])

        self.assertEqual(context["300389"]["open_date"], "2026-03-20")
        self.assertEqual(context["300389"]["first_buy_price"], 18.8)
        self.assertEqual(context["300389"]["net_shares"], 500)

    def test_shadow_advisory_signals_cover_time_stop_and_drawdown(self):
        from datetime import date

        from scripts.pipeline.shadow_trade import _build_advisory_signals

        advisory = _build_advisory_signals(
            position={"code": "300389", "name": "艾比森", "shares": 1000, "cost": 10.0, "price": 10.1},
            trade_context={"open_date": "2026-03-01", "first_buy_price": 10.0, "net_shares": 1000},
            history_points=[
                {"date": "2026-03-03", "close": 10.2},
                {"date": "2026-03-10", "close": 11.6},
                {"date": "2026-03-20", "close": 12.0},
                {"date": "2026-04-08", "close": 10.1},
            ],
            risk_cfg={
                "time_stop_days": 15,
                "take_profit": {"t1_pct": 0.15, "t1_drawdown": 0.05, "t2_drawdown": 0.08},
            },
            today=date(2026, 4, 9),
        )

        rule_codes = {signal["rule_code"] for signal in advisory["signals"]}
        self.assertIn("RISK_TIME_STOP", rule_codes)
        self.assertIn("RISK_DRAWDOWN_TAKE_PROFIT", rule_codes)
        self.assertEqual(advisory["open_date"], "2026-03-01")
        self.assertEqual(advisory["hold_days"], 39)
        self.assertAlmostEqual(advisory["drawdown_pct"], 0.1583, places=4)

    def test_combined_state_audit_includes_paper_trade_consistency(self):
        from scripts.cli.trade import _combined_state_audit

        with mock.patch("scripts.cli.trade.audit_state", return_value={
            "status": "ok",
            "snapshot_date": "2026-04-09",
            "checks": {"stocks_yaml": {"ok": True}},
        }), mock.patch("scripts.cli.trade._shadow_trade_snapshot", return_value={
            "status": "drift",
            "consistency": {
                "ok": False,
                "status": "drift",
                "event_only_codes": ["300389"],
                "broker_only_codes": [],
            },
        }):
            result = _combined_state_audit()

        self.assertEqual(result["status"], "drift")
        self.assertIn("paper_trade_consistency", result["checks"])
        self.assertEqual(
            result["checks"]["paper_trade_consistency"]["event_only_codes"],
            ["300389"],
        )

    def test_paper_trade_consistency_snapshot_detects_share_mismatch(self):
        from scripts.pipeline.shadow_trade import paper_trade_consistency_snapshot

        with mock.patch("scripts.pipeline.shadow_trade.load_activity_summary", return_value={
            "trade_count": 1,
            "trade_events": [
                {
                    "event_date": "2026-04-01",
                    "side": "buy",
                    "code": "300389",
                    "name": "艾比森",
                    "shares": 1000,
                    "price": 19.13,
                }
            ],
        }), mock.patch("scripts.pipeline.shadow_trade.get_status", return_value={
            "positions": [
                {"code": "300389", "name": "艾比森", "shares": 600, "cost": 19.2, "price": 19.5}
            ],
        }):
            snapshot = paper_trade_consistency_snapshot(window=30)

        self.assertEqual(snapshot["status"], "drift")
        self.assertEqual(snapshot["event_only_codes"], [])
        self.assertEqual(snapshot["broker_only_codes"], [])
        self.assertEqual(len(snapshot["share_mismatches"]), 1)
        self.assertEqual(snapshot["share_mismatches"][0]["delta_shares"], -400)

    def test_reconcile_trade_state_builds_actions_without_applying(self):
        from scripts.pipeline.shadow_trade import reconcile_trade_state

        with mock.patch("scripts.pipeline.shadow_trade.paper_trade_consistency_snapshot", return_value={
            "ok": False,
            "status": "drift",
            "event_trade_count": 2,
            "event_only_codes": ["300389"],
            "broker_only_codes": ["603063"],
            "share_mismatches": [],
            "inferred_positions": {
                "300389": {
                    "code": "300389",
                    "name": "艾比森",
                    "shares": 1000,
                    "avg_cost": 19.13,
                    "first_buy_price": 19.13,
                }
            },
            "actual_positions": {
                "603063": {
                    "code": "603063",
                    "name": "禾望电气",
                    "shares": 600,
                    "cost": 32.76,
                    "price": 33.10,
                }
            },
        }):
            result = reconcile_trade_state(apply=False, window=30)

        self.assertEqual(result["status"], "drift")
        self.assertEqual(result["planned_action_count"], 2)
        self.assertEqual(result["applied_actions"], [])
        self.assertEqual({item["reason_code"] for item in result["planned_actions"]}, {
            "PAPER_RECONCILE_FLATTEN",
            "PAPER_RECONCILE_OPEN",
        })

    def test_reconcile_trade_state_apply_logs_synthetic_events(self):
        from scripts.pipeline.shadow_trade import reconcile_trade_state

        consistency_before = {
            "ok": False,
            "status": "drift",
            "event_trade_count": 1,
            "event_only_codes": ["300389"],
            "broker_only_codes": [],
            "share_mismatches": [],
            "inferred_positions": {
                "300389": {
                    "code": "300389",
                    "name": "艾比森",
                    "shares": 1000,
                    "avg_cost": 19.13,
                    "first_buy_price": 19.13,
                }
            },
            "actual_positions": {},
        }
        consistency_after = {
            "ok": True,
            "status": "ok",
            "event_trade_count": 2,
            "event_only_codes": [],
            "broker_only_codes": [],
            "share_mismatches": [],
            "inferred_positions": {},
            "actual_positions": {},
        }

        with mock.patch("scripts.pipeline.shadow_trade.paper_trade_consistency_snapshot", side_effect=[
            consistency_before,
            consistency_after,
        ]), mock.patch("scripts.pipeline.shadow_trade._log_trade") as log_trade:
            result = reconcile_trade_state(apply=True, window=30)

        self.assertEqual(result["status"], "ok")
        self.assertEqual(len(result["applied_actions"]), 1)
        log_trade.assert_called_once()
        self.assertEqual(result["consistency_after"]["status"], "ok")

    def test_status_today_exposes_shadow_trade_summary(self):
        from scripts.cli.trade import status_today

        with mock.patch("scripts.cli.trade.load_daily_state", return_value={
            "date": "2026-04-09",
            "updated_at": "2026-04-09T10:00:00",
            "pipelines": {},
        }), mock.patch("scripts.cli.trade.get_strategy", return_value={}), mock.patch(
            "scripts.cli.trade.build_today_decision",
            return_value={"action": "CLEAR", "weekly_buys": 0},
        ), mock.patch("scripts.cli.trade.load_portfolio_snapshot", return_value={
            "summary": {"holding_count": 0, "current_exposure": 0.0},
        }), mock.patch("scripts.cli.trade.load_pool_snapshot", return_value={
            "updated_at": "2026-04-09T09:30:00",
            "snapshot_date": "2026-04-09",
            "summary": {"core_count": 2, "watch_count": 5, "other_count": 0},
        }), mock.patch("scripts.cli.trade.audit_state", return_value={
            "status": "ok",
            "checks": {},
        }), mock.patch("scripts.cli.trade.load_market_snapshot", return_value={
            "signal": "CLEAR",
            "source": "market_timer",
            "source_chain": ["market_timer"],
            "as_of_date": "2026-04-09",
        }), mock.patch("scripts.cli.trade._shadow_trade_snapshot", return_value={
            "status": "drift",
            "timestamp": "2026-04-09 10:00",
            "positions_count": 1,
            "automation_scope": "advisory only",
            "advisory_summary": {
                "triggered_signal_count": 2,
                "triggered_position_count": 1,
                "triggered_rules": ["RISK_TIME_STOP", "RISK_DRAWDOWN_TAKE_PROFIT"],
                "positions": [{"code": "300389", "summary": "test"}],
            },
            "consistency": {
                "ok": False,
                "status": "drift",
                "event_only_codes": ["300389"],
                "broker_only_codes": [],
            },
        }):
            result = status_today(sync_state=False)

        self.assertEqual(result["paper_trade_audit"]["status"], "drift")
        self.assertEqual(result["shadow_trade_state"]["positions_count"], 1)
        self.assertEqual(
            result["shadow_trade_state"]["advisory_summary"]["triggered_signal_count"],
            2,
        )


if __name__ == "__main__":
    unittest.main()
