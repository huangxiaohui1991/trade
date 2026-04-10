import contextlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock


class BacktestRunnerTests(unittest.TestCase):
    def test_run_backtest_summarizes_structured_inputs(self):
        from scripts.backtest import run_backtest

        with tempfile.TemporaryDirectory() as tmpdir, mock.patch("scripts.backtest.runner.BACKTEST_SAMPLE_DIR", Path(tmpdir)), mock.patch("scripts.backtest.runner.BACKTEST_REPORT_DIR", Path(tmpdir)), mock.patch("scripts.backtest.runner.BACKTEST_INDEX_PATH", Path(tmpdir) / "index.json"), mock.patch("scripts.backtest.runner.load_trade_review", return_value={
            "scope": "cn_a_system",
            "window": 30,
            "closed_trade_count": 2,
            "win_count": 1,
            "loss_count": 1,
            "win_rate": 50.0,
            "total_realized_pnl": 860.0,
            "open_position_count": 0,
            "closed_trades": [
                {
                    "code": "300389",
                    "exit_date": "2026-04-03",
                    "realized_pnl": 1200.0,
                    "entry_score": 7.8,
                    "exit_reason_codes": ["RISK_TAKE_PROFIT_T1"],
                },
                {
                    "code": "603063",
                    "exit_date": "2026-04-07",
                    "realized_pnl": -340.0,
                    "entry_score": 6.1,
                    "exit_reason_codes": ["RISK_ABSOLUTE_STOP_LOSS"],
                },
            ],
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
        self.assertEqual(result["sample_count"], 1)
        self.assertEqual(result["score_summary"]["win_rate"], 50.0)
        self.assertEqual(result["score_summary"]["pool_core_count"], 2)
        self.assertEqual(result["risk_summary"]["portfolio_risk_state"], "block")
        self.assertEqual(result["state_fields"]["market"]["signal"], "GREEN")
        self.assertEqual(result["selected_parameters"]["buy_threshold"], 7.0)
        self.assertEqual(result["score_summary"]["selected_summary"]["closed_trade_count"], 1)
        self.assertGreater(len(result["parameter_rankings"]), 0)
        self.assertEqual(result["sample_store"]["sample_count"], 2)
        self.assertTrue(result["sample_store"]["path"])
        self.assertTrue(result["result_path"])
        self.assertTrue(result["report_path"])
        self.assertEqual(len(result["artifacts"]), 3)

    def test_walk_forward_uses_fixture_input(self):
        from scripts.backtest import run_walk_forward

        fixture_payload = {
            "trade_review": {
                "scope": "cn_a_system",
                "window": 9,
                "closed_trade_count": 3,
                "win_count": 2,
                "loss_count": 1,
                "win_rate": 66.7,
                "total_realized_pnl": 1800.0,
                "open_position_count": 0,
                "closed_trades": [
                    {
                        "code": "300389",
                        "exit_date": "2026-04-03",
                        "realized_pnl": 1200.0,
                        "entry_score": 7.8,
                        "exit_reason_codes": ["RISK_TAKE_PROFIT_T1"],
                    },
                    {
                        "code": "603063",
                        "exit_date": "2026-04-06",
                        "realized_pnl": -300.0,
                        "entry_score": 6.3,
                        "exit_reason_codes": ["RISK_ABSOLUTE_STOP_LOSS"],
                    },
                    {
                        "code": "000612",
                        "exit_date": "2026-04-09",
                        "realized_pnl": 900.0,
                        "entry_score": 8.0,
                        "exit_reason_codes": ["POOL_DEMOTE"],
                    }
                ],
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
            with mock.patch("scripts.backtest.runner.BACKTEST_SAMPLE_DIR", Path(tmpdir)), mock.patch("scripts.backtest.runner.BACKTEST_REPORT_DIR", Path(tmpdir)), mock.patch("scripts.backtest.runner.BACKTEST_INDEX_PATH", Path(tmpdir) / "index.json"):
                result = run_walk_forward("2026-04-01", "2026-04-09", folds=3, fixture=fixture_path)

        self.assertEqual(result["command"], "backtest")
        self.assertEqual(result["action"], "walk-forward")
        self.assertEqual(result["parameters"]["source_mode"], "fixture")
        self.assertGreaterEqual(result["parameters"]["folds"], 1)
        self.assertGreaterEqual(result["sample_count"], 1)
        self.assertEqual(len(result["folds"]), result["parameters"]["folds"])
        self.assertIn("mean_win_rate", result["score_summary"])
        self.assertIn("worst_risk_state", result["risk_summary"])
        self.assertIn("training_summary", result["folds"][0]["score_summary"])
        self.assertIn("evaluation_summary", result["folds"][0]["score_summary"])
        self.assertIn("selected_parameters", result["folds"][0]["score_summary"])
        self.assertIn("portfolio_replay_summary", result["folds"][0])
        self.assertEqual(result["sample_store"]["sample_count"], 3)
        self.assertTrue(result["sample_store"]["path"])
        self.assertTrue(result["result_path"])
        self.assertTrue(result["report_path"])
        self.assertEqual(result["comparison_summary"]["fold_count"], result["parameters"]["folds"])
        self.assertEqual(len(result["comparison_summary"]["rows"]), result["parameters"]["folds"])
        self.assertIn("evaluation_peak_exposure_pct", result["comparison_summary"]["rows"][0])

    def test_walk_forward_report_includes_fold_comparison_tables(self):
        from scripts.backtest import run_walk_forward

        fixture_payload = {
            "trade_review": {
                "scope": "cn_a_system",
                "window": 9,
                "closed_trade_count": 3,
                "win_count": 2,
                "loss_count": 1,
                "win_rate": 66.7,
                "total_realized_pnl": 1800.0,
                "open_position_count": 0,
                "closed_trades": [
                    {
                        "code": "300389",
                        "exit_date": "2026-04-03",
                        "realized_pnl": 1200.0,
                        "entry_score": 7.8,
                        "exit_reason_codes": ["RISK_TAKE_PROFIT_T1"],
                    },
                    {
                        "code": "603063",
                        "exit_date": "2026-04-06",
                        "realized_pnl": -300.0,
                        "entry_score": 6.3,
                        "exit_reason_codes": ["RISK_ABSOLUTE_STOP_LOSS"],
                    },
                    {
                        "code": "000612",
                        "exit_date": "2026-04-09",
                        "realized_pnl": 900.0,
                        "entry_score": 8.0,
                        "exit_reason_codes": ["POOL_DEMOTE"],
                    },
                ],
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
            with mock.patch("scripts.backtest.runner.BACKTEST_SAMPLE_DIR", Path(tmpdir)), mock.patch("scripts.backtest.runner.BACKTEST_REPORT_DIR", Path(tmpdir)), mock.patch("scripts.backtest.runner.BACKTEST_INDEX_PATH", Path(tmpdir) / "index.json"):
                result = run_walk_forward("2026-04-01", "2026-04-09", folds=3, fixture=fixture_path)
            report_text = Path(result["report_path"]).read_text(encoding="utf-8")

        self.assertIn("## Fold Comparison", report_text)
        self.assertIn("## Aggregate Comparison", report_text)
        self.assertIn("Selected Parameters", report_text)
        self.assertIn("Train PnL", report_text)
        self.assertIn("Eval PnL", report_text)
        self.assertIn("Best Eval Fold", report_text)
        self.assertEqual(result["comparison_summary"]["fold_count"], 3)
        self.assertEqual(len(result["comparison_summary"]["rows"]), 3)
        self.assertTrue(result["comparison_summary"]["rows"][0]["selected_parameters"])

    def test_run_parameter_sweep_ranks_candidates(self):
        from scripts.backtest import run_parameter_sweep

        fixture_payload = {
            "trade_review": {
                "scope": "cn_a_system",
                "window": 9,
                "closed_trade_count": 2,
                "win_count": 1,
                "loss_count": 1,
                "win_rate": 50.0,
                "total_realized_pnl": 900.0,
                "open_position_count": 0,
                "closed_trades": [
                    {
                        "code": "300389",
                        "exit_date": "2026-04-03",
                        "realized_pnl": 1200.0,
                        "entry_score": 7.8,
                        "exit_reason_codes": ["RISK_TAKE_PROFIT_T1"],
                    },
                    {
                        "code": "603063",
                        "exit_date": "2026-04-06",
                        "realized_pnl": -300.0,
                        "entry_score": 6.2,
                        "exit_reason_codes": ["RISK_ABSOLUTE_STOP_LOSS"],
                    },
                ],
                "open_positions": [],
                "source": "fixture",
                "mfe_mae_status": "pending_market_history",
            },
            "pool_snapshot": {"summary": {"core_count": 1, "watch_count": 1}},
            "market_snapshot": {"signal": "GREEN", "as_of_date": "2026-04-09", "source": "fixture"},
            "state_audit": {"status": "ok", "snapshot_date": "2026-04-09"},
            "today_decision": {
                "action": "NO_TRADE",
                "portfolio_risk": {"state": "ok", "reason_codes": [], "reasons": []},
            },
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            fixture_path = Path(tmpdir) / "backtest_fixture.json"
            fixture_path.write_text(json.dumps(fixture_payload, ensure_ascii=False), encoding="utf-8")
            with mock.patch("scripts.backtest.runner.BACKTEST_SAMPLE_DIR", Path(tmpdir)), mock.patch("scripts.backtest.runner.BACKTEST_REPORT_DIR", Path(tmpdir)), mock.patch("scripts.backtest.runner.BACKTEST_INDEX_PATH", Path(tmpdir) / "index.json"):
                result = run_parameter_sweep(
                    "2026-04-01",
                    "2026-04-09",
                    fixture=fixture_path,
                    buy_thresholds="6,7,8",
                    stop_losses="0.03,0.04",
                    take_profits="0.15,0.2",
                )

        self.assertEqual(result["action"], "sweep")
        self.assertEqual(result["ranking_count"], 12)
        self.assertGreater(len(result["rankings"]), 0)
        self.assertIn("buy_threshold", result["selected_parameters"])
        self.assertEqual(result["sample_store"]["sample_count"], 2)
        self.assertTrue(result["sample_store"]["path"])
        self.assertTrue(result["result_path"])
        self.assertTrue(result["report_path"])

    def test_parameter_sweep_uses_history_replay_when_available(self):
        from scripts.backtest import run_parameter_sweep

        fixture_payload = {
            "trade_review": {
                "scope": "cn_a_system",
                "window": 5,
                "closed_trade_count": 1,
                "win_count": 1,
                "loss_count": 0,
                "win_rate": 100.0,
                "total_realized_pnl": 1000.0,
                "open_position_count": 0,
                "closed_trades": [
                    {
                        "code": "300389",
                        "name": "艾比森",
                        "entry_date": "2026-04-01",
                        "exit_date": "2026-04-03",
                        "entry_price": 10.0,
                        "exit_price": 10.4,
                        "realized_pnl": 400.0,
                        "shares": 1000,
                        "entry_score": 8.0,
                        "history_rows": [
                            {"日期": "2026-04-01", "最高": 10.8, "最低": 9.8},
                            {"日期": "2026-04-02", "最高": 11.4, "最低": 10.2},
                            {"日期": "2026-04-03", "最高": 11.1, "最低": 10.7},
                        ],
                        "exit_reason_codes": ["RISK_TAKE_PROFIT_T1"],
                    }
                ],
                "open_positions": [],
                "source": "fixture",
                "mfe_mae_status": "actual_market_history",
            },
            "pool_snapshot": {"summary": {"core_count": 1, "watch_count": 0}},
            "market_snapshot": {"signal": "GREEN", "as_of_date": "2026-04-09", "source": "fixture"},
            "state_audit": {"status": "ok", "snapshot_date": "2026-04-09"},
            "today_decision": {
                "action": "NO_TRADE",
                "portfolio_risk": {"state": "ok", "reason_codes": [], "reasons": []},
            },
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            fixture_path = Path(tmpdir) / "backtest_fixture.json"
            fixture_path.write_text(json.dumps(fixture_payload, ensure_ascii=False), encoding="utf-8")
            with mock.patch("scripts.backtest.runner.BACKTEST_SAMPLE_DIR", Path(tmpdir)), mock.patch("scripts.backtest.runner.BACKTEST_REPORT_DIR", Path(tmpdir)), mock.patch("scripts.backtest.runner.BACKTEST_INDEX_PATH", Path(tmpdir) / "index.json"):
                result = run_parameter_sweep(
                    "2026-04-01",
                    "2026-04-03",
                    fixture=fixture_path,
                    buy_thresholds="7",
                    stop_losses="0.03",
                    take_profits="0.1,0.15",
                )

        ranked = {item["params"]["take_profit"]: item["summary"]["total_realized_pnl"] for item in result["rankings"]}
        self.assertEqual(ranked[0.1], 1000.0)
        self.assertEqual(ranked[0.15], 400.0)
        self.assertEqual(result["sample_store"]["source"], "actual_market_history")

    def test_parameter_sweep_recomputes_entry_score_from_weight_grid(self):
        from scripts.backtest import run_parameter_sweep

        fixture_payload = {
            "trade_review": {
                "scope": "cn_a_system",
                "window": 5,
                "closed_trade_count": 1,
                "win_count": 1,
                "loss_count": 0,
                "win_rate": 100.0,
                "total_realized_pnl": 500.0,
                "open_position_count": 0,
                "closed_trades": [
                    {
                        "code": "300389",
                        "name": "艾比森",
                        "entry_date": "2026-04-01",
                        "exit_date": "2026-04-03",
                        "entry_price": 10.0,
                        "exit_price": 10.5,
                        "realized_pnl": 500.0,
                        "shares": 1000,
                        "technical_score": 2.0,
                        "fundamental_score": 2.0,
                        "flow_score": 0.0,
                        "sentiment_score": 0.0,
                        "entry_score": 4.0,
                        "exit_reason_codes": ["POOL_DEMOTE"],
                    }
                ],
                "open_positions": [],
                "source": "fixture",
                "mfe_mae_status": "proxy_market_history",
            },
            "pool_snapshot": {"summary": {"core_count": 1, "watch_count": 0}},
            "market_snapshot": {"signal": "GREEN", "as_of_date": "2026-04-09", "source": "fixture"},
            "state_audit": {"status": "ok", "snapshot_date": "2026-04-09"},
            "today_decision": {
                "action": "NO_TRADE",
                "portfolio_risk": {"state": "ok", "reason_codes": [], "reasons": []},
            },
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            fixture_path = Path(tmpdir) / "backtest_fixture.json"
            fixture_path.write_text(json.dumps(fixture_payload, ensure_ascii=False), encoding="utf-8")
            with mock.patch("scripts.backtest.runner.BACKTEST_SAMPLE_DIR", Path(tmpdir)), mock.patch("scripts.backtest.runner.BACKTEST_REPORT_DIR", Path(tmpdir)), mock.patch("scripts.backtest.runner.BACKTEST_INDEX_PATH", Path(tmpdir) / "index.json"), mock.patch("scripts.backtest.runner._load_trade_history_for_replay", return_value=[]):
                result = run_parameter_sweep(
                    "2026-04-01",
                    "2026-04-03",
                    fixture=fixture_path,
                    buy_thresholds="5",
                    technical_weights="2,5",
                    fundamental_weights="3,0",
                    flow_weights="0",
                    sentiment_weights="0",
                )

        ranked = {item["params"]["technical_weight"]: item["summary"]["closed_trade_count"] for item in result["rankings"]}
        self.assertEqual(ranked[2.0], 0)
        self.assertEqual(ranked[5.0], 1)

    def test_list_backtest_history_reads_latest_entries(self):
        from scripts.backtest import list_backtest_history

        with tempfile.TemporaryDirectory() as tmpdir:
            index_path = Path(tmpdir) / "index.json"
            index_path.write_text(json.dumps([
                {"action": "walk_forward", "status": "warning", "created_at": "2026-04-09T23:00:00"},
                {"action": "run", "status": "ok", "created_at": "2026-04-09T22:00:00"},
            ], ensure_ascii=False), encoding="utf-8")
            with mock.patch("scripts.backtest.runner.BACKTEST_INDEX_PATH", index_path):
                result = list_backtest_history(limit=1)

        self.assertEqual(result["command"], "backtest")
        self.assertEqual(result["action"], "history")
        self.assertEqual(result["item_count"], 1)
        self.assertEqual(result["items"][0]["action"], "walk_forward")

    def test_compare_backtest_history_builds_leaderboard(self):
        from scripts.backtest import compare_backtest_history

        with tempfile.TemporaryDirectory() as tmpdir:
            index_path = Path(tmpdir) / "index.json"
            index_path.write_text(json.dumps([
                {
                    "action": "run",
                    "status": "ok",
                    "engine_mode": "proxy_parameter_sweep",
                    "excursion_source": "actual_market_history",
                    "scope": "cn_a_system",
                    "sample_count": 3,
                    "total_realized_pnl": 1200.0,
                    "win_rate": 66.7,
                },
                {
                    "action": "walk_forward",
                    "status": "warning",
                    "engine_mode": "proxy_walk_forward",
                    "excursion_source": "proxy_market_history",
                    "scope": "cn_a_system",
                    "sample_count": 5,
                    "total_realized_pnl": 800.0,
                    "win_rate": 75.0,
                },
            ], ensure_ascii=False), encoding="utf-8")
            with mock.patch("scripts.backtest.runner.BACKTEST_INDEX_PATH", index_path):
                result = compare_backtest_history(limit=10)

        self.assertEqual(result["command"], "backtest")
        self.assertEqual(result["action"], "compare")
        self.assertEqual(result["item_count"], 2)
        self.assertEqual(result["summary"]["action_counts"]["run"], 1)
        self.assertEqual(result["summary"]["action_counts"]["walk_forward"], 1)
        self.assertEqual(result["summary"]["excursion_source_counts"]["actual_market_history"], 1)
        self.assertEqual(result["leaders"]["best_pnl"]["total_realized_pnl"], 1200.0)
        self.assertEqual(result["leaders"]["best_win_rate"]["win_rate"], 75.0)
        self.assertEqual(result["leaders"]["largest_sample"]["sample_count"], 5)

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
        sweep_payload = {
            "command": "backtest",
            "action": "sweep",
            "status": "ok",
            "parameters": {"start": "2026-04-01", "end": "2026-04-09"},
            "sample_count": 2,
            "baseline_parameters": {"buy_threshold": 7.0, "stop_loss": 0.04, "take_profit": 0.15},
            "selected_parameters": {"buy_threshold": 6.0, "stop_loss": 0.03, "take_profit": 0.2},
            "ranking_count": 4,
            "rankings": [],
        }
        history_payload = {
            "command": "backtest",
            "action": "history",
            "status": "ok",
            "index_path": "/tmp/index.json",
            "item_count": 1,
            "items": [{"action": "run", "status": "ok"}],
        }
        compare_payload = {
            "command": "backtest",
            "action": "compare",
            "status": "ok",
            "item_count": 2,
            "leaders": {
                "best_pnl": {"total_realized_pnl": 1200.0},
                "best_win_rate": {"win_rate": 75.0},
                "largest_sample": {"sample_count": 5},
            },
            "items": [],
        }

        stdout = io.StringIO()
        with mock.patch.object(trade, "run_backtest", return_value=run_payload) as run_mock, mock.patch.object(
            trade,
            "run_walk_forward",
            return_value=walk_payload,
        ) as walk_mock, mock.patch.object(
            trade,
            "run_parameter_sweep",
            return_value=sweep_payload,
        ) as sweep_mock, mock.patch.object(trade.sys, "argv", ["trade", "--json", "backtest", "run", "--start", "2026-04-01", "--end", "2026-04-09", "--fixture", "/tmp/fixture.json"]):
            with contextlib.redirect_stdout(stdout):
                trade.main()

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["command"], "backtest")
        self.assertEqual(payload["action"], "run")
        run_mock.assert_called_once()
        self.assertEqual(run_mock.call_args.kwargs["technical_weights"], None)
        self.assertEqual(run_mock.call_args.kwargs["fixture"], "/tmp/fixture.json")
        self.assertEqual(run_mock.call_args.kwargs["buy_thresholds"], None)

        stdout = io.StringIO()
        with mock.patch.object(trade, "run_backtest", return_value=run_payload), mock.patch.object(
            trade,
            "run_walk_forward",
            return_value=walk_payload,
        ) as walk_mock, mock.patch.object(
            trade,
            "run_parameter_sweep",
            return_value=sweep_payload,
        ), mock.patch.object(trade.sys, "argv", ["trade", "backtest", "walk-forward", "--start", "2026-04-01", "--end", "2026-04-09", "--json"]):
            with contextlib.redirect_stdout(stdout):
                trade.main()

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["command"], "backtest")
        self.assertEqual(payload["action"], "walk-forward")
        walk_mock.assert_called_once()
        self.assertEqual(walk_mock.call_args.kwargs["folds"], 3)

        stdout = io.StringIO()
        with mock.patch.object(trade, "run_backtest", return_value=run_payload), mock.patch.object(
            trade,
            "run_walk_forward",
            return_value=walk_payload,
        ), mock.patch.object(
            trade,
            "run_parameter_sweep",
            return_value=sweep_payload,
        ) as sweep_mock, mock.patch.object(
            trade.sys,
            "argv",
            [
                "trade",
                "--json",
                "backtest",
                "sweep",
                "--start",
                "2026-04-01",
                "--end",
                "2026-04-09",
                "--buy-thresholds",
                "6,7,8",
            ],
        ):
            with contextlib.redirect_stdout(stdout):
                trade.main()

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["command"], "backtest")
        self.assertEqual(payload["action"], "sweep")
        sweep_mock.assert_called_once()

        stdout = io.StringIO()
        with mock.patch.object(trade, "run_backtest", return_value=run_payload) as run_mock, mock.patch.object(
            trade,
            "run_walk_forward",
            return_value=walk_payload,
        ), mock.patch.object(
            trade,
            "run_parameter_sweep",
            return_value=sweep_payload,
        ) as sweep_mock, mock.patch.object(trade.sys, "argv", [
            "trade",
            "--json",
            "backtest",
            "sweep",
            "--start",
            "2026-04-01",
            "--end",
            "2026-04-09",
            "--technical-weights",
            "2,5",
            "--fundamental-weights",
            "3,0",
            "--flow-weights",
            "0",
            "--sentiment-weights",
            "0",
        ]):
            with contextlib.redirect_stdout(stdout):
                trade.main()

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["command"], "backtest")
        self.assertEqual(payload["action"], "sweep")
        self.assertEqual(sweep_mock.call_args.kwargs["technical_weights"], "2,5")
        self.assertEqual(sweep_mock.call_args.kwargs["fundamental_weights"], "3,0")
        self.assertEqual(sweep_mock.call_args.kwargs["flow_weights"], "0")
        self.assertEqual(sweep_mock.call_args.kwargs["sentiment_weights"], "0")

        stdout = io.StringIO()
        with mock.patch.object(trade, "run_backtest", return_value=run_payload), mock.patch.object(
            trade,
            "run_walk_forward",
            return_value=walk_payload,
        ), mock.patch.object(
            trade,
            "run_parameter_sweep",
            return_value=sweep_payload,
        ), mock.patch.object(
            trade,
            "list_backtest_history",
            return_value=history_payload,
        ) as history_mock, mock.patch.object(trade.sys, "argv", ["trade", "--json", "backtest", "history", "--limit", "5"]):
            with contextlib.redirect_stdout(stdout):
                trade.main()

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["command"], "backtest")
        self.assertEqual(payload["action"], "history")
        history_mock.assert_called_once()

        stdout = io.StringIO()
        with mock.patch.object(trade, "run_backtest", return_value=run_payload), mock.patch.object(
            trade,
            "run_walk_forward",
            return_value=walk_payload,
        ), mock.patch.object(
            trade,
            "run_parameter_sweep",
            return_value=sweep_payload,
        ), mock.patch.object(
            trade,
            "list_backtest_history",
            return_value=history_payload,
        ), mock.patch.object(
            trade,
            "compare_backtest_history",
            return_value=compare_payload,
        ) as compare_mock, mock.patch.object(trade.sys, "argv", ["trade", "--json", "backtest", "compare", "--limit", "5"]):
            with contextlib.redirect_stdout(stdout):
                trade.main()

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["command"], "backtest")
        self.assertEqual(payload["action"], "compare")
        compare_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
