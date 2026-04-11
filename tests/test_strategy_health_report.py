"""Tests for strategy health report aggregation and CLI dispatch."""

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts.backtest import historical_pipeline
from scripts.backtest.historical_pipeline import (
    render_strategy_health_report,
    run_strategy_health_report,
)
from scripts.cli import trade


class StrategyHealthReportTests(unittest.TestCase):

    def test_run_strategy_health_report_derives_codes_from_pool_history(self):
        snapshots = [
            {
                "snapshot_date": "2026-04-01",
                "pipeline": "stock_screener",
                "entries": [
                    {"code": "AAA", "bucket": "core"},
                    {"code": "BBB", "bucket": "core"},
                ],
            }
        ]
        pool_result = {
            "status": "ok",
            "findings": ["核心池后 20 日平均收益 +6.20%"],
            "window_statistics": [{"window_days": 20, "avg_return_pct": 6.2, "positive_rate_pct": 60.0}],
        }
        veto_result = {
            "status": "ok",
            "findings": ["limit_up_today 偏严"],
            "effective_rules": [{"rule": "red_market", "pure_risk_intercept_rate_pct": 30.0, "pure_false_kill_rate_pct": 10.0}],
            "too_strict_rules": [{"rule": "limit_up_today", "pure_false_kill_rate_pct": 60.0, "pure_risk_intercept_rate_pct": 8.0}],
        }
        batch_result = {
            "status": "ok",
            "aggregate": {
                "stock_count": 2,
                "closed_trade_count": 6,
                "total_realized_pnl": 3200.0,
                "blended_win_rate": 66.7,
                "worst_max_drawdown_pct": -4.2,
            }
        }

        with mock.patch.object(historical_pipeline, "_load_pool_snapshots_for_range", return_value=snapshots), mock.patch.object(
            historical_pipeline,
            "run_pool_entry_performance_analysis",
            return_value=pool_result,
        ) as pool_mock, mock.patch.object(
            historical_pipeline,
            "run_veto_rule_analysis",
            return_value=veto_result,
        ) as veto_mock, mock.patch.object(
            historical_pipeline,
            "run_multi_stock_system_backtest",
            return_value=batch_result,
        ) as batch_mock:
            result = run_strategy_health_report(
                start="2026-04-01",
                end="2026-04-10",
                bucket="core",
                holding_windows=[5, 10, 20],
                pipeline="stock_screener",
                code_limit=30,
                sample_limit=5,
            )

        self.assertEqual(result["action"], "strategy_health_report")
        self.assertEqual(result["selected_codes"], ["AAA", "BBB"])
        self.assertEqual(result["coverage"]["available_code_count"], 2)
        pool_mock.assert_called_once()
        veto_mock.assert_called_once_with(
            stock_codes=["AAA", "BBB"],
            start="2026-04-01",
            end="2026-04-10",
            total_capital=None,
            strategy_params=None,
            lookahead_days=20,
            opportunity_gain_pct=0.15,
            risk_drawdown_pct=0.08,
            sample_limit=5,
        )
        batch_mock.assert_called_once_with(
            stock_codes=["AAA", "BBB"],
            start="2026-04-01",
            end="2026-04-10",
            total_capital=None,
            strategy_params=None,
        )
        text = render_strategy_health_report(result)
        self.assertIn("Strategy Health", text)
        self.assertIn("AAA", str(result["selected_codes"]))

    def test_cli_backtest_strategy_health_json_contract(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            params_path = Path(tmpdir) / "strategy_health_params.json"
            params_path.write_text(json.dumps({"entry_mode": "hybrid"}), encoding="utf-8")
            output_path = Path(tmpdir) / "strategy_health.json"
            fake_result = {
                "command": "backtest",
                "action": "strategy_health_report",
                "status": "ok",
                "coverage": {"selected_code_count": 5},
                "findings": ["批量回放 5 只股票"],
                "pool_performance": {},
                "veto_analysis": {},
                "batch_backtest": {},
            }

            with mock.patch.object(trade, "run_strategy_health_report", return_value=dict(fake_result)) as run_mock:
                payload = self._run_main(
                    [
                        "trade",
                        "--json",
                        "backtest",
                        "strategy-health",
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
                        "--code-limit",
                        "20",
                        "--capital",
                        "500000",
                        "--preset",
                        "aggressive_high_return",
                        "--params-json",
                        str(params_path),
                        "--lookahead-days",
                        "30",
                        "--opportunity-gain-pct",
                        "0.2",
                        "--risk-drawdown-pct",
                        "0.1",
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
                code_limit=20,
                total_capital=500000.0,
                strategy_params={
                    "entry_mode": "hybrid",
                    "preset": "aggressive_high_return",
                },
                veto_lookahead_days=30,
                veto_opportunity_gain_pct=0.2,
                veto_risk_drawdown_pct=0.1,
                sample_limit=7,
            )
            self.assertEqual(payload["action"], "strategy_health_report")
            self.assertEqual(payload["report_path"], str(output_path))
            self.assertEqual(json.loads(output_path.read_text(encoding="utf-8")), fake_result)

    def test_cli_backtest_strategy_health_renders_report(self):
        payload = {
            "command": "backtest",
            "action": "strategy_health_report",
            "status": "ok",
            "start": "2026-04-01",
            "end": "2026-04-10",
            "bucket": "core",
            "pipeline": "stock_screener",
            "coverage": {"available_code_count": 5, "selected_code_count": 5},
            "findings": ["批量回放 5 只股票"],
            "batch_backtest": {"aggregate": {"total_realized_pnl": 3200.0, "closed_trade_count": 6, "blended_win_rate": 66.7, "worst_max_drawdown_pct": -4.2}},
            "pool_performance": {"window_statistics": []},
            "veto_analysis": {"effective_rules": [], "too_strict_rules": []},
            "report_path": "/tmp/strategy_health.json",
        }

        stdout = io.StringIO()
        with mock.patch.object(
            trade,
            "run_strategy_health_report",
            return_value=dict(payload),
        ) as run_mock, mock.patch(
            "scripts.backtest.historical_pipeline.render_strategy_health_report",
            return_value="STRATEGY HEALTH",
        ) as render_mock, mock.patch.object(
            trade.sys,
            "argv",
            [
                "trade",
                "backtest",
                "strategy-health",
                "--start",
                "2026-04-01",
                "--end",
                "2026-04-10",
            ],
        ):
            with contextlib.redirect_stdout(stdout):
                trade.main()

        output = stdout.getvalue()
        self.assertIn("STRATEGY HEALTH", output)
        self.assertIn("report_path: /tmp/strategy_health.json", output)
        run_mock.assert_called_once()
        render_mock.assert_called_once_with(payload)

    def _run_main(self, argv: list[str]) -> dict:
        stdout = io.StringIO()
        with mock.patch.object(trade.sys, "argv", argv), contextlib.redirect_stdout(stdout):
            trade.main()
        return json.loads(stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
