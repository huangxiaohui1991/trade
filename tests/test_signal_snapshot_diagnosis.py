import contextlib
import io
import json
import os
import tempfile
import unittest
from unittest import mock


class SignalSnapshotDiagnosisTests(unittest.TestCase):
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

    def _seed_history_bundle(self, history_group_id: str = "screener:2026-04-11:150001") -> None:
        from scripts.state import (
            save_candidate_snapshot_history,
            save_decision_snapshot_history,
            save_market_snapshot_history,
            save_pool_snapshot,
        )

        with mock.patch("scripts.state.service._project_stocks_yaml", return_value="/tmp/stocks.yaml"), mock.patch(
            "scripts.state.service.ObsidianVault",
            return_value=mock.Mock(sync_pool_projection=mock.Mock(return_value={})),
        ):
            save_pool_snapshot(
                [
                    {
                        "code": "601869",
                        "name": "长飞光纤",
                        "bucket": "core",
                        "total_score": 7.6,
                        "technical_score": 2.8,
                        "fundamental_score": 1.5,
                        "flow_score": 1.2,
                        "sentiment_score": 2.1,
                        "veto_triggered": False,
                        "veto_signals": [],
                        "data_quality": "ok",
                        "note": "连续2天分数>=7.0",
                    },
                    {
                        "code": "603108",
                        "name": "润达医疗",
                        "bucket": "watch",
                        "total_score": 5.4,
                        "technical_score": 2.0,
                        "fundamental_score": 1.3,
                        "flow_score": 0.9,
                        "sentiment_score": 1.2,
                        "veto_triggered": False,
                        "veto_signals": [],
                        "data_quality": "ok",
                        "note": "观察池继续跟踪",
                    },
                ],
                metadata={
                    "snapshot_date": "2026-04-11",
                    "updated_at": "2026-04-11T15:00:01",
                    "pipeline": "stock_screener",
                    "history_group_id": history_group_id,
                    "source": "unit_test",
                    "pool": "all",
                    "universe": "tracked",
                },
            )

        save_market_snapshot_history(
            {
                "as_of_date": "2026-04-11",
                "updated_at": "2026-04-11T15:00:01",
                "signal": "GREEN",
                "source": "unit_test_market",
                "source_chain": ["unit_test_market"],
                "indices": {"上证指数": {"signal": "GREEN"}},
            },
            pipeline="stock_screener",
            history_group_id=history_group_id,
            metadata={"snapshot_date": "2026-04-11"},
        )
        save_decision_snapshot_history(
            {
                "decision": "BUY",
                "action": "BUY",
                "market_signal": "GREEN",
                "portfolio_risk": {"state": "ok"},
            },
            snapshot_date="2026-04-11",
            pipeline="stock_screener",
            history_group_id=history_group_id,
            metadata={"snapshot_date": "2026-04-11"},
        )
        save_candidate_snapshot_history(
            [
                {
                    "code": "601869",
                    "name": "长飞光纤",
                    "total_score": 7.6,
                    "technical_score": 2.8,
                    "fundamental_score": 1.5,
                    "flow_score": 1.2,
                    "sentiment_score": 2.1,
                    "veto_triggered": False,
                    "veto_signals": [],
                    "data_quality": "ok",
                },
                {
                    "code": "603108",
                    "name": "润达医疗",
                    "total_score": 5.4,
                    "technical_score": 2.0,
                    "fundamental_score": 1.3,
                    "flow_score": 0.9,
                    "sentiment_score": 1.2,
                    "veto_triggered": False,
                    "veto_signals": [],
                    "data_quality": "ok",
                },
            ],
            snapshot_date="2026-04-11",
            pipeline="stock_screener",
            history_group_id=history_group_id,
            pool="all",
            universe="tracked",
            source="unit_test",
            actionable_count=2,
            metadata={"snapshot_date": "2026-04-11"},
        )

    def test_diagnose_signal_snapshot_reports_selected_stock(self):
        from scripts.backtest.historical_pipeline import diagnose_signal_snapshot, render_signal_snapshot_diagnosis_report

        self._seed_history_bundle()
        report = diagnose_signal_snapshot("2026-04-11", stock_code="601869")

        self.assertEqual(report["command"], "backtest")
        self.assertEqual(report["action"], "signal_snapshot_diagnosis")
        self.assertEqual(report["status"], "ok")
        self.assertEqual(report["today_decision"]["action"], "BUY")
        self.assertEqual(report["market_snapshot"]["signal"], "GREEN")
        self.assertEqual(report["candidate_count"], 2)
        self.assertGreaterEqual(len(report["available_history_groups"]), 1)
        self.assertGreaterEqual(len(report["market_timeline"]), 1)
        self.assertEqual(report["available_history_groups"][0]["timepoint"], "screener")
        self.assertEqual(report["code_diagnosis"]["status"], "selected_core")
        self.assertEqual(report["code_diagnosis"]["pool_bucket"], "core")
        self.assertTrue(report["code_diagnosis"]["candidate_present"])
        self.assertEqual(report["code_diagnosis_across_groups"][0]["status"], "selected_core")
        text = render_signal_snapshot_diagnosis_report(report)
        self.assertIn("历史信号镜像诊断", text)
        self.assertIn("selected_core", text)
        self.assertIn("时点摘要", text)

    def test_diagnose_signal_snapshot_compares_code_across_groups(self):
        from scripts.backtest.historical_pipeline import diagnose_signal_snapshot
        from scripts.state import save_market_snapshot_history

        self._seed_history_bundle()
        save_market_snapshot_history(
            {
                "as_of_date": "2026-04-11",
                "updated_at": "2026-04-11T12:00:00",
                "signal": "YELLOW",
                "source": "midday_market",
                "source_chain": ["midday_market"],
                "indices": {"上证指数": {"signal": "YELLOW"}},
            },
            pipeline="noon",
            history_group_id="noon:2026-04-11:midday:120000",
            metadata={"snapshot_date": "2026-04-11", "timepoint": "midday"},
        )

        report = diagnose_signal_snapshot("2026-04-11", stock_code="601869")

        statuses = {
            item["timepoint"]: item["status"]
            for item in report["code_diagnosis_across_groups"]
        }
        self.assertEqual(statuses["screener"], "selected_core")
        self.assertEqual(statuses["midday"], "candidate_snapshot_missing")
        timeline_timepoints = [item["timepoint"] for item in report["market_timeline"]]
        self.assertIn("midday", timeline_timepoints)
        self.assertIn("screener", timeline_timepoints)

    def test_diagnose_signal_snapshot_reports_missed_stock(self):
        from scripts.backtest.historical_pipeline import diagnose_signal_snapshot

        self._seed_history_bundle()
        report = diagnose_signal_snapshot("2026-04-11", stock_code="002962")

        self.assertEqual(report["status"], "ok")
        self.assertEqual(report["code_diagnosis"]["status"], "not_in_scored_candidates")
        self.assertFalse(report["code_diagnosis"]["candidate_present"])
        self.assertFalse(report["code_diagnosis"]["pool_present"])

    def test_cli_backtest_signal_diagnose_json_dispatches(self):
        import scripts.cli.trade as trade

        fake_result = {
            "command": "backtest",
            "action": "signal_snapshot_diagnosis",
            "status": "ok",
            "snapshot_date": "2026-04-11",
            "history_group_id": "screener:2026-04-11:150001",
            "available_history_groups": [],
            "market_snapshot": {"signal": "GREEN"},
            "today_decision": {"action": "BUY"},
            "candidate_snapshot": {"candidate_count": 2, "candidates": []},
            "pool_snapshot": {"entries": [], "summary": {"core_count": 1, "watch_count": 1}},
            "candidate_count": 2,
            "pool_entry_count": 2,
            "code_diagnosis": {"code": "601869", "status": "selected_core"},
        }

        stdout = io.StringIO()
        with mock.patch.object(trade, "diagnose_signal_snapshot", return_value=dict(fake_result)) as diagnose_mock, mock.patch.object(
            trade.sys,
            "argv",
            [
                "trade",
                "--json",
                "backtest",
                "signal-diagnose",
                "--date",
                "2026-04-11",
                "--history-group-id",
                "screener:2026-04-11:150001",
                "--code",
                "601869",
                "--candidate-limit",
                "10",
            ],
        ):
            with contextlib.redirect_stdout(stdout):
                trade.main()

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["action"], "signal_snapshot_diagnosis")
        diagnose_mock.assert_called_once_with(
            snapshot_date="2026-04-11",
            history_group_id="screener:2026-04-11:150001",
            stock_code="601869",
            candidate_limit=10,
        )


if __name__ == "__main__":
    unittest.main()
