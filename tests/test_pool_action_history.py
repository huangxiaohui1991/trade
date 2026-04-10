import os
import tempfile
import unittest
from unittest import mock


class PoolActionHistoryTests(unittest.TestCase):
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

    def test_save_pool_snapshot_records_action_history(self):
        from scripts.state import load_pool_action_history, save_pool_snapshot

        self._bootstrap_empty()
        with mock.patch("scripts.state.service._project_stocks_yaml", return_value="/tmp/stocks.yaml"), mock.patch(
            "scripts.state.service.ObsidianVault",
            return_value=mock.Mock(sync_pool_projection=mock.Mock(return_value={})),
        ):
            save_pool_snapshot(
                [
                    {"code": "300389", "name": "艾比森", "bucket": "watch", "total_score": 6.2},
                    {"code": "603063", "name": "禾望电气", "bucket": "core", "total_score": 7.3},
                ],
                metadata={"snapshot_date": "2026-04-09", "source": "unit_test"},
            )
            save_pool_snapshot(
                [
                    {"code": "300389", "name": "艾比森", "bucket": "core", "total_score": 7.6},
                    {"code": "603063", "name": "禾望电气", "bucket": "watch", "total_score": 5.4},
                    {"code": "000612", "name": "焦作万方", "bucket": "watch", "total_score": 5.8},
                ],
                metadata={"snapshot_date": "2026-04-10", "source": "unit_test"},
            )

        history = load_pool_action_history(snapshot_date="2026-04-10")
        actions = {item["code"]: item["action"] for item in history["actions"]}

        self.assertEqual(history["snapshot_date"], "2026-04-10")
        self.assertEqual(actions["300389"], "promote")
        self.assertEqual(actions["603063"], "demote")
        self.assertEqual(actions["000612"], "keep")
        self.assertEqual(history["action_counts"]["promote"], 1)
        self.assertEqual(history["action_counts"]["demote"], 1)

    def test_pool_snapshot_preserves_data_quality_metadata(self):
        from scripts.state import load_pool_snapshot, save_pool_snapshot

        self._bootstrap_empty()
        with mock.patch("scripts.state.service._project_stocks_yaml", return_value="/tmp/stocks.yaml"), mock.patch(
            "scripts.state.service.ObsidianVault",
            return_value=mock.Mock(sync_pool_projection=mock.Mock(return_value={})),
        ):
            save_pool_snapshot(
                [
                    {
                        "code": "300389",
                        "name": "艾比森",
                        "bucket": "core",
                        "total_score": 6.8,
                        "data_quality": "degraded",
                        "data_missing_fields": ["营收", "现金流"],
                    },
                ],
                metadata={"snapshot_date": "2026-04-10", "source": "unit_test"},
            )

        entry = load_pool_snapshot()["core_pool"][0]
        self.assertEqual(entry["data_quality"], "degraded")
        self.assertEqual(entry["data_missing_fields"], ["营收", "现金流"])
        self.assertEqual(entry["metadata"]["data_quality"], "degraded")

    def test_audit_state_reports_projection_drift_details(self):
        from scripts.state import audit_state, save_pool_snapshot

        self._bootstrap_empty()
        with mock.patch("scripts.state.service._project_stocks_yaml", return_value="/tmp/stocks.yaml"), mock.patch(
            "scripts.state.service.ObsidianVault",
            return_value=mock.Mock(sync_pool_projection=mock.Mock(return_value={})),
        ):
            save_pool_snapshot(
                [
                    {"code": "300389", "name": "艾比森", "bucket": "core", "total_score": 7.6},
                    {"code": "603063", "name": "禾望电气", "bucket": "watch", "total_score": 5.4},
                ],
                metadata={"snapshot_date": "2026-04-10", "source": "unit_test"},
            )

        with mock.patch("scripts.state.service.get_stocks", return_value={
            "core_pool": [{"code": "300389", "name": "艾比森", "score": 7.0}],
            "watch_pool": [{"code": "000612", "name": "焦作万方", "score": 5.8}],
        }), mock.patch("scripts.state.service._load_md_rows", side_effect=lambda path: [
            {"代码": "300389", "四维总分": "7.6"},
            {"代码": "603063", "四维总分": "5.4"},
        ] if "核心池" in path else []):
            result = audit_state()

        self.assertEqual(result["status"], "drift")
        stocks_check = result["checks"]["stocks_yaml"]
        self.assertEqual(stocks_check["missing_codes"], ["603063"])
        self.assertEqual(stocks_check["extra_codes"], ["000612"])
        self.assertEqual(stocks_check["score_mismatches"][0]["code"], "300389")
        obsidian_check = result["checks"]["obsidian_projection"]
        self.assertEqual(obsidian_check["bucket_mismatches"][0]["code"], "603063")


if __name__ == "__main__":
    unittest.main()
