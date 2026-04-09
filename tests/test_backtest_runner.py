import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock


class BacktestRunnerTests(unittest.TestCase):
    def test_run_backtest_summarizes_structured_inputs(self):
        from scripts.backtest import run_backtest

        with mock.patch("scripts.backtest.runner.load_trade_review", return_value={
            "scope": "cn_a_system",
            "window": 30,
            "closed_trade_count": 2,
            "win_count": 1,
            "loss_count": 1,
            "win_rate": 50.0,
            "total_realized_pnl": 860.0,
            "open_position_count": 0,
            "closed_trades": [],
            "open_positions": [],
            "source": "structured_ledger",
            "mfe_mae_status": "pending_market_history",
        }), mock.patch("scripts.backtest.runner.load_pool_snapshot", return_value={
            "summary": {"core_count": 2, "watch_count": 3},
            "core_pool": [{"code": "300389"}, {"code": "603063"}],
            "watch_pool": [{"code": "000001"}, {"code": "000002"}, {"code": "000003"}],
        }), mock.patch("scripts.backtest.runner.load_market_snapshot", return_value={
            "signal": "GREEN",
            "source": "market_timer",
            "as_of_date": "2026-04-09",
        }), mock.patch("scripts.backtest.runner.audit_state", return_value={
            "status": "ok",
            "snapshot_date": "2026-04-09",
        }), mock.patch("scripts.backtest.runner.build_today_decision", return_value={
            "action": "NO_TRADE",
            "portfolio_risk": {
                "state": "block",
                "reason_codes": ["TRADE_CONSECUTIVE_LOSS_COOLDOWN"],
                "reasons": ["连续亏损冷却中"],
            },
        }), mock.patch("scripts.backtest.runner.get_strategy", return_value={}):
            result = run_backtest("2026-04-01", "2026-04-09", scope="cn_a_system")

        self.assertEqual(result["command"], "backtest")
        self.assertEqual(result["action"], "run")
        self.assertEqual(result["status"], "warning")
        self.assertEqual(result["sample_count"], 2)
        self.assertEqual(result["score_summary"]["win_rate"], 50.0)
        self.assertEqual(result["score_summary"]["pool_core_count"], 2)
        self.assertEqual(result["risk_summary"]["portfolio_risk_state"], "block")
        self.assertEqual(result["state_fields"]["market"]["signal"], "GREEN")

    def test_walk_forward_uses_fixture_input(self):
        from scripts.backtest import run_walk_forward

        fixture_payload = {
            "trade_review": {
                "scope": "cn_a_system",
                "window": 9,
                "closed_trade_count": 1,
                "win_count": 1,
                "loss_count": 0,
                "win_rate": 100.0,
                "total_realized_pnl": 2200.0,
                "open_position_count": 0,
                "closed_trades": [],
                "open_positions": [],
                "source": "fixture",
                "mfe_mae_status": "pending_market_history",
            },
            "pool_snapshot": {"summary": {"core_count": 1, "watch_count": 1}},
            "market_snapshot": {"signal": "CLEAR", "as_of_date": "2026-04-09", "source": "fixture"},
            "state_audit": {"status": "ok", "snapshot_date": "2026-04-09"},
            "today_decision": {
                "action": "NO_TRADE",
                "portfolio_risk": {"state": "ok", "reason_codes": [], "reasons": []},
            },
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            fixture_path = Path(tmpdir) / "backtest_fixture.json"
            fixture_path.write_text(json.dumps(fixture_payload, ensure_ascii=False), encoding="utf-8")
            result = run_walk_forward("2026-04-01", "2026-04-09", folds=3, fixture=fixture_path)

        self.assertEqual(result["command"], "backtest")
        self.assertEqual(result["action"], "walk-forward")
        self.assertEqual(result["parameters"]["source_mode"], "fixture")
        self.assertGreaterEqual(result["parameters"]["folds"], 1)
        self.assertGreaterEqual(result["sample_count"], 1)
        self.assertEqual(len(result["folds"]), result["parameters"]["folds"])
        self.assertIn("mean_win_rate", result["score_summary"])
        self.assertIn("worst_risk_state", result["risk_summary"])

    def test_cli_backtest_commands_parse_and_dispatch(self):
        import scripts.cli.trade as trade

        run_payload = {
            "command": "backtest",
            "action": "run",
            "status": "ok",
            "parameters": {"start": "2026-04-01", "end": "2026-04-09"},
            "sample_count": 1,
            "score_summary": {"win_rate": 100.0, "total_realized_pnl": 2200.0},
            "risk_summary": {"risk_state": "ok"},
            "state_fields": {},
        }
        walk_payload = {
            "command": "backtest",
            "action": "walk-forward",
            "status": "warning",
            "parameters": {"start": "2026-04-01", "end": "2026-04-09", "folds": 2},
            "sample_count": 2,
            "score_summary": {"mean_win_rate": 75.0, "total_realized_pnl": 1800.0},
            "risk_summary": {"worst_risk_state": "warning"},
            "state_fields": {},
            "folds": [],
        }

        stdout = io.StringIO()
        with mock.patch.object(trade, "run_backtest", return_value=run_payload) as run_mock, mock.patch.object(
            trade,
            "run_walk_forward",
            return_value=walk_payload,
        ) as walk_mock, mock.patch.object(trade.sys, "argv", ["trade", "--json", "backtest", "run", "--start", "2026-04-01", "--end", "2026-04-09", "--fixture", "/tmp/fixture.json"]):
            with contextlib.redirect_stdout(stdout):
                trade.main()

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["command"], "backtest")
        self.assertEqual(payload["action"], "run")
        run_mock.assert_called_once()
        self.assertEqual(run_mock.call_args.kwargs["fixture"], "/tmp/fixture.json")

        stdout = io.StringIO()
        with mock.patch.object(trade, "run_backtest", return_value=run_payload), mock.patch.object(
            trade,
            "run_walk_forward",
            return_value=walk_payload,
        ) as walk_mock, mock.patch.object(trade.sys, "argv", ["trade", "backtest", "walk-forward", "--start", "2026-04-01", "--end", "2026-04-09", "--json"]):
            with contextlib.redirect_stdout(stdout):
                trade.main()

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["command"], "backtest")
        self.assertEqual(payload["action"], "walk-forward")
        walk_mock.assert_called_once()
        self.assertEqual(walk_mock.call_args.kwargs["folds"], 3)


if __name__ == "__main__":
    unittest.main()
