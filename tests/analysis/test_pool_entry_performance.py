"""Tests for pool entry forward performance analysis and CLI dispatch."""

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

from scripts.backtest import historical_pipeline
from scripts.backtest.historical_pipeline import (
    render_pool_entry_performance_report,
    run_pool_entry_performance_analysis,
)
from scripts.cli import trade


def _price_frame(rows: list[tuple[str, float, float, float]]) -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "date": pd.to_datetime([row[0] for row in rows]),
            "close": [row[1] for row in rows],
            "high": [row[2] for row in rows],
            "low": [row[3] for row in rows],
        }
    )
    frame["_date_str"] = frame["date"].dt.strftime("%Y-%m-%d")
    frame.attrs["date_index"] = {value: idx for idx, value in enumerate(frame["_date_str"].tolist())}
    return frame


class PoolEntryPerformanceTests(unittest.TestCase):

    def test_run_pool_entry_performance_uses_fresh_entries_only(self):
        snapshots = [
            {
                "snapshot_date": "2026-04-01",
                "history_group_id": "grp-001",
                "pipeline": "stock_screener",
                "updated_at": "2026-04-01T15:00:00",
                "entries": [
                    {"code": "AAA", "name": "Alpha", "bucket": "core", "total_score": 7.8},
                ],
            },
            {
                "snapshot_date": "2026-04-02",
                "history_group_id": "grp-002",
                "pipeline": "stock_screener",
                "updated_at": "2026-04-02T15:00:00",
                "entries": [
                    {"code": "AAA", "name": "Alpha", "bucket": "core", "total_score": 7.7},
                ],
            },
            {
                "snapshot_date": "2026-04-03",
                "history_group_id": "grp-003",
                "pipeline": "stock_screener",
                "updated_at": "2026-04-03T15:00:00",
                "entries": [
                    {"code": "AAA", "name": "Alpha", "bucket": "core", "total_score": 7.6},
                    {"code": "BBB", "name": "Beta", "bucket": "core", "total_score": 7.1},
                ],
            },
        ]
        price_frames = {
            "AAA": _price_frame(
                [
                    ("2026-04-01", 10.0, 10.0, 9.8),
                    ("2026-04-02", 11.0, 11.3, 10.6),
                    ("2026-04-03", 12.0, 12.2, 11.4),
                    ("2026-04-04", 13.0, 13.4, 12.6),
                    ("2026-04-05", 14.0, 14.5, 13.6),
                ]
            ),
            "BBB": _price_frame(
                [
                    ("2026-04-01", 19.5, 19.8, 19.0),
                    ("2026-04-02", 20.0, 20.2, 19.7),
                    ("2026-04-03", 20.0, 20.0, 19.8),
                    ("2026-04-04", 19.0, 19.4, 18.7),
                    ("2026-04-05", 18.0, 18.5, 17.2),
                ]
            ),
        }

        with mock.patch.object(historical_pipeline, "_load_pool_snapshots_for_range", return_value=snapshots), mock.patch.object(
            historical_pipeline,
            "_load_price_frames_for_codes",
            return_value=price_frames,
        ):
            result = run_pool_entry_performance_analysis(
                start="2026-04-01",
                end="2026-04-03",
                bucket="core",
                holding_windows=[2],
                pipeline="stock_screener",
                sample_limit=3,
            )

        self.assertEqual(result["action"], "pool_entry_performance")
        self.assertEqual(result["coverage"]["snapshot_count"], 3)
        self.assertEqual(result["coverage"]["entry_event_count"], 2)
        self.assertEqual(result["coverage"]["priced_event_count"], 2)
        self.assertEqual(result["window_statistics"][0]["window_days"], 2)
        self.assertEqual(result["window_statistics"][0]["sample_count"], 2)
        self.assertEqual(result["window_statistics"][0]["avg_return_pct"], 5.0)
        self.assertEqual(result["window_statistics"][0]["positive_rate_pct"], 50.0)
        self.assertEqual(result["top_stocks"][0]["code"], "AAA")
        self.assertEqual(result["bottom_stocks"][0]["code"], "BBB")
        self.assertEqual(result["top_events"][0]["code"], "AAA")
        self.assertEqual(result["bottom_events"][0]["code"], "BBB")

        report = render_pool_entry_performance_report(result)
        self.assertIn("核心池", report)
        self.assertIn("AAA", report)
        self.assertIn("BBB", report)

    def test_cli_backtest_pool_performance_json_contract(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "pool_report.json"
            fake_result = {
                "command": "backtest",
                "action": "pool_entry_performance",
                "status": "ok",
                "coverage": {"snapshot_count": 8, "entry_event_count": 4},
                "window_statistics": [{"window_days": 20, "avg_return_pct": 6.2}],
                "findings": [],
                "top_stocks": [],
                "bottom_stocks": [],
                "top_events": [],
                "bottom_events": [],
            }

            with mock.patch.object(trade, "run_pool_entry_performance_analysis", return_value=dict(fake_result)) as run_mock:
                payload = self._run_main(
                    [
                        "trade",
                        "--json",
                        "backtest",
                        "pool-performance",
                        "--start",
                        "2026-04-01",
                        "--end",
                        "2026-04-10",
                        "--bucket",
                        "core",
                        "--codes",
                        "601869,002962",
                        "--pipeline",
                        "stock_screener",
                        "--windows",
                        "5,10,20",
                        "--sample-limit",
                        "7",
                        "--output",
                        str(output_path),
                    ]
                )

            run_mock.assert_called_once_with(
                start="2026-04-01",
                end="2026-04-10",
                bucket="core",
                holding_windows=[5, 10, 20],
                stock_codes=["601869", "002962"],
                pipeline="stock_screener",
                sample_limit=7,
            )
            self.assertEqual(payload["action"], "pool_entry_performance")
            self.assertEqual(payload["report_path"], str(output_path))
            self.assertEqual(json.loads(output_path.read_text(encoding="utf-8")), fake_result)

    def test_cli_backtest_pool_performance_renders_report(self):
        payload = {
            "command": "backtest",
            "action": "pool_entry_performance",
            "status": "ok",
            "start": "2026-04-01",
            "end": "2026-04-10",
            "bucket": "core",
            "pipeline": "stock_screener",
            "holding_windows": [5, 10, 20],
            "coverage": {"snapshot_count": 8, "entry_event_count": 4, "priced_event_count": 4},
            "window_statistics": [],
            "findings": ["核心池后 20 日平均收益 +6.20%"],
            "top_stocks": [],
            "bottom_stocks": [],
            "top_events": [],
            "bottom_events": [],
            "report_path": "/tmp/pool_performance.json",
        }

        stdout = io.StringIO()
        with mock.patch.object(
            trade,
            "run_pool_entry_performance_analysis",
            return_value=dict(payload),
        ) as run_mock, mock.patch(
            "scripts.backtest.historical_pipeline.render_pool_entry_performance_report",
            return_value="POOL REPORT",
        ) as render_mock, mock.patch.object(
            trade.sys,
            "argv",
            [
                "trade",
                "backtest",
                "pool-performance",
                "--start",
                "2026-04-01",
                "--end",
                "2026-04-10",
            ],
        ):
            with contextlib.redirect_stdout(stdout):
                trade.main()

        output = stdout.getvalue()
        self.assertIn("POOL REPORT", output)
        self.assertIn("report_path: /tmp/pool_performance.json", output)
        run_mock.assert_called_once()
        render_mock.assert_called_once_with(payload)

    def _run_main(self, argv: list[str]) -> dict:
        stdout = io.StringIO()
        with mock.patch.object(trade.sys, "argv", argv), contextlib.redirect_stdout(stdout):
            trade.main()
        return json.loads(stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
