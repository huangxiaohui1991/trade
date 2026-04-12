"""Tests for veto rule analysis and CLI dispatch."""

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts.backtest import historical_pipeline
from scripts.backtest.historical_pipeline import (
    render_veto_rule_analysis_report,
    run_veto_rule_analysis,
)
from scripts.cli import trade


def _candidate(code: str, score: float, veto_signals: list[str]) -> dict:
    return {
        "code": code,
        "name": code,
        "score": score,
        "price": 10.0,
        "entry_signal": True,
        "entry_reasons": ["strict"],
        "veto_signals": veto_signals,
    }


class VetoRuleAnalysisTests(unittest.TestCase):

    def test_run_veto_rule_analysis_separates_effective_and_strict_rules(self):
        fixture = {
            "daily_data": {
                "2026-04-01": {
                    "market_signal": "GREEN",
                    "candidates": [_candidate("AAA", 8.0, ["below_ma20"])],
                    "prices": {"AAA": 10.0},
                    "bars": {"AAA": {"close": 10.0, "high": 10.0, "low": 10.0}},
                },
                "2026-04-02": {
                    "market_signal": "GREEN",
                    "candidates": [],
                    "prices": {"AAA": 9.0},
                    "bars": {"AAA": {"close": 9.0, "high": 10.2, "low": 8.8}},
                },
                "2026-04-03": {
                    "market_signal": "GREEN",
                    "candidates": [],
                    "prices": {"AAA": 8.5},
                    "bars": {"AAA": {"close": 8.5, "high": 9.2, "low": 8.0}},
                },
                "2026-04-04": {
                    "market_signal": "RED",
                    "candidates": [_candidate("AAA", 8.1, ["red_market"])],
                    "prices": {"AAA": 10.0},
                    "bars": {"AAA": {"close": 10.0, "high": 10.0, "low": 10.0}},
                },
                "2026-04-05": {
                    "market_signal": "GREEN",
                    "candidates": [],
                    "prices": {"AAA": 12.0},
                    "bars": {"AAA": {"close": 12.0, "high": 12.5, "low": 9.9}},
                },
                "2026-04-06": {
                    "market_signal": "GREEN",
                    "candidates": [],
                    "prices": {"AAA": 12.8},
                    "bars": {"AAA": {"close": 12.8, "high": 13.0, "low": 11.8}},
                },
                "2026-04-07": {
                    "market_signal": "RED",
                    "candidates": [_candidate("AAA", 8.2, ["below_ma20", "red_market"])],
                    "prices": {"AAA": 10.0},
                    "bars": {"AAA": {"close": 10.0, "high": 10.0, "low": 10.0}},
                },
                "2026-04-08": {
                    "market_signal": "GREEN",
                    "candidates": [],
                    "prices": {"AAA": 8.9},
                    "bars": {"AAA": {"close": 8.9, "high": 11.6, "low": 8.7}},
                },
                "2026-04-09": {
                    "market_signal": "GREEN",
                    "candidates": [],
                    "prices": {"AAA": 11.8},
                    "bars": {"AAA": {"close": 11.8, "high": 12.0, "low": 8.2}},
                },
            },
            "params": {
                "buy_threshold": 7,
                "require_entry_signal": True,
                "veto_rules": ["below_ma20", "red_market"],
            },
            "_meta": {
                "data_fidelity": {
                    "mode": "historical_signal_mirror",
                    "history_days": 9,
                    "proxy_days": 0,
                }
            },
        }

        with mock.patch.object(historical_pipeline, "build_replay_fixture", return_value=fixture):
            result = run_veto_rule_analysis(
                stock_codes=["AAA"],
                start="2026-04-01",
                end="2026-04-09",
                total_capital=100000.0,
                lookahead_days=2,
                opportunity_gain_pct=0.15,
                risk_drawdown_pct=0.08,
            )

        self.assertEqual(result["action"], "veto_rule_analysis")
        self.assertEqual(result["coverage"]["stock_count"], 1)
        self.assertEqual(result["coverage"]["trigger_count"], 4)
        self.assertEqual(result["summary"]["opportunity_only_count"], 1)
        self.assertEqual(result["summary"]["risk_only_count"], 1)
        self.assertEqual(result["summary"]["both_hit_count"], 2)
        self.assertEqual(result["effective_rules"][0]["rule"], "below_ma20")
        self.assertEqual(result["too_strict_rules"][0]["rule"], "red_market")
        self.assertEqual(result["top_missed_opportunities"][0]["rule"], "red_market")
        self.assertEqual(result["top_blocked_losses"][0]["rule"], "below_ma20")

        report = render_veto_rule_analysis_report(result)
        self.assertIn("below_ma20", report)
        self.assertIn("red_market", report)

    def test_cli_backtest_veto_analysis_json_contract(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            params_path = Path(tmpdir) / "veto_params.json"
            params_path.write_text(json.dumps({"entry_mode": "hybrid"}), encoding="utf-8")
            output_path = Path(tmpdir) / "veto_report.json"
            fake_result = {
                "command": "backtest",
                "action": "veto_rule_analysis",
                "status": "ok",
                "coverage": {"stock_count": 2, "trigger_count": 18},
                "summary": {"pure_false_kill_rate_pct": 22.2},
                "effective_rules": [],
                "too_strict_rules": [],
                "top_missed_opportunities": [],
                "top_blocked_losses": [],
            }

            with mock.patch.object(trade, "run_veto_rule_analysis", return_value=dict(fake_result)) as run_mock:
                payload = self._run_main(
                    [
                        "trade",
                        "--json",
                        "backtest",
                        "veto-analysis",
                        "--codes",
                        "601869,002962",
                        "--start",
                        "2025-04-11",
                        "--end",
                        "2026-04-10",
                        "--index",
                        "system",
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
                stock_codes=["601869", "002962"],
                start="2025-04-11",
                end="2026-04-10",
                index_code="system",
                total_capital=500000.0,
                strategy_params={
                    "entry_mode": "hybrid",
                    "preset": "aggressive_high_return",
                },
                lookahead_days=30,
                opportunity_gain_pct=0.2,
                risk_drawdown_pct=0.1,
                sample_limit=7,
            )
            self.assertEqual(payload["action"], "veto_rule_analysis")
            self.assertEqual(payload["report_path"], str(output_path))
            self.assertEqual(json.loads(output_path.read_text(encoding="utf-8")), fake_result)

    def test_cli_backtest_veto_analysis_renders_report(self):
        payload = {
            "command": "backtest",
            "action": "veto_rule_analysis",
            "status": "ok",
            "coverage": {"stock_count": 1, "veto_day_count": 3, "trigger_count": 4, "history_days": 9, "proxy_days": 0},
            "summary": {
                "pure_risk_intercept_rate_pct": 25.0,
                "pure_false_kill_rate_pct": 25.0,
                "mixed_rate_pct": 50.0,
                "avg_peak_gain_pct": 13.0,
                "avg_worst_drawdown_pct": -9.0,
            },
            "analysis_window": {"lookahead_days": 20, "opportunity_gain_pct": 0.15, "risk_drawdown_pct": 0.08},
            "effective_rules": [],
            "too_strict_rules": [],
            "top_missed_opportunities": [],
            "top_blocked_losses": [],
            "findings": ["red_market 偏严"],
            "start": "2025-04-11",
            "end": "2026-04-10",
            "report_path": "/tmp/veto_analysis.json",
        }

        stdout = io.StringIO()
        with mock.patch.object(
            trade,
            "run_veto_rule_analysis",
            return_value=dict(payload),
        ) as run_mock, mock.patch(
            "scripts.backtest.historical_pipeline.render_veto_rule_analysis_report",
            return_value="VETO REPORT",
        ) as render_mock, mock.patch.object(
            trade.sys,
            "argv",
            [
                "trade",
                "backtest",
                "veto-analysis",
                "--code",
                "601869",
                "--start",
                "2025-04-11",
                "--end",
                "2026-04-10",
            ],
        ):
            with contextlib.redirect_stdout(stdout):
                trade.main()

        output = stdout.getvalue()
        self.assertIn("VETO REPORT", output)
        self.assertIn("report_path: /tmp/veto_analysis.json", output)
        run_mock.assert_called_once()
        render_mock.assert_called_once_with(payload)

    def _run_main(self, argv: list[str]) -> dict:
        stdout = io.StringIO()
        with mock.patch.object(trade.sys, "argv", argv), contextlib.redirect_stdout(stdout):
            trade.main()
        return json.loads(stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
