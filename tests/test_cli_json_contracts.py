import contextlib
import io
import json
import unittest
from pathlib import Path
from unittest import mock


FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "cli_contracts"


def _load_fixture(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


class CLIJsonContractTests(unittest.TestCase):
    def _run_main(self, argv: list[str], patches: list[mock._patch]) -> dict:
        import scripts.cli.trade as trade

        stdout = io.StringIO()
        with contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch.object(trade.sys, "argv", argv))
            for patch in patches:
                stack.enter_context(patch)
            with contextlib.redirect_stdout(stdout):
                trade.main()
        return json.loads(stdout.getvalue())

    def _doctor_contract(self, payload: dict) -> dict:
        checks = payload["checks"]
        return {
            "command": payload["command"],
            "status": payload["status"],
            "retryable": payload["retryable"],
            "hard_fail": payload["hard_fail"],
            "warning": payload["warning"],
            "checks": {
                "python": {
                    "ok": checks["python"]["ok"],
                    "version": checks["python"]["version"],
                },
                "mx_apikey": {
                    "ok": checks["mx_apikey"]["ok"],
                    "configured": checks["mx_apikey"]["configured"],
                },
                "discord_webhook": {
                    "ok": checks["discord_webhook"]["ok"],
                    "configured": checks["discord_webhook"]["configured"],
                },
                "daily_state": {
                    "ok": checks["daily_state"]["ok"],
                    "date": checks["daily_state"]["date"],
                    "pipelines": checks["daily_state"]["pipelines"],
                },
                "state_audit": {
                    "ok": checks["state_audit"]["ok"],
                    "status": checks["state_audit"]["status"],
                    "snapshot_date": checks["state_audit"]["snapshot_date"],
                },
                "mx_connectivity": {
                    "ok": checks["mx_connectivity"]["ok"],
                },
                "akshare_connectivity": {
                    "ok": checks["akshare_connectivity"]["ok"],
                },
                "vault": {
                    "ok": checks["vault"]["ok"],
                },
                "writable": {
                    "ledger": {
                        "ok": checks["writable"]["ledger"]["ok"],
                    }
                },
            },
        }

    def _state_audit_contract(self, payload: dict) -> dict:
        return {
            "command": payload["command"],
            "action": payload["action"],
            "status": payload["status"],
            "db_path": payload["db_path"],
            "snapshot_date": payload["snapshot_date"],
            "checks": {
                "stocks_yaml": {
                    "ok": payload["checks"]["stocks_yaml"]["ok"],
                },
                "obsidian_projection": {
                    "ok": payload["checks"]["obsidian_projection"]["ok"],
                },
                "paper_trade_consistency": {
                    "ok": payload["checks"]["paper_trade_consistency"]["ok"],
                    "status": payload["checks"]["paper_trade_consistency"]["status"],
                    "event_only_codes": payload["checks"]["paper_trade_consistency"]["event_only_codes"],
                    "broker_only_codes": payload["checks"]["paper_trade_consistency"]["broker_only_codes"],
                },
            },
        }

    def _state_reconcile_contract(self, payload: dict) -> dict:
        def _items(values: list[dict]) -> list[dict]:
            return [
                {
                    "action": item["action"],
                    "side": item["side"],
                    "code": item["code"],
                    "shares": item["shares"],
                    "reason_code": item["reason_code"],
                }
                for item in values
            ]

        return {
            "command": payload["command"],
            "action": payload["action"],
            "status": payload["status"],
            "db_path": payload["db_path"],
            "apply": payload["apply"],
            "planned_action_count": payload["planned_action_count"],
            "planned_actions": _items(payload["planned_actions"]),
            "applied_actions": _items(payload["applied_actions"]),
            "consistency_before": {
                "status": payload["consistency_before"]["status"],
                "event_only_codes": payload["consistency_before"]["event_only_codes"],
                "broker_only_codes": payload["consistency_before"]["broker_only_codes"],
            },
            "consistency_after": {
                "status": payload["consistency_after"]["status"],
                "event_only_codes": payload["consistency_after"]["event_only_codes"],
                "broker_only_codes": payload["consistency_after"]["broker_only_codes"],
            },
        }

    def _status_today_contract(self, payload: dict) -> dict:
        return {
            "command": payload["command"],
            "date": payload["date"],
            "updated_at": payload["updated_at"],
            "pipelines": {
                "morning": {
                    "status": payload["pipelines"]["morning"]["status"],
                    "updated_at": payload["pipelines"]["morning"]["updated_at"],
                }
            },
            "today_decision": {
                "action": payload["today_decision"]["action"],
                "weekly_buys": payload["today_decision"]["weekly_buys"],
            },
            "positions_summary": {
                "holding_count": payload["positions_summary"]["holding_count"],
                "current_exposure": payload["positions_summary"]["current_exposure"],
            },
            "market_signal": payload["market_signal"],
            "market_snapshot_source": {
                "source": payload["market_snapshot_source"]["source"],
                "source_chain": payload["market_snapshot_source"]["source_chain"],
                "as_of_date": payload["market_snapshot_source"]["as_of_date"],
            },
            "order_snapshot": {
                "scope": payload["order_snapshot"]["scope"],
                "status": payload["order_snapshot"]["status"],
                "summary": {
                    "order_count": payload["order_snapshot"]["summary"]["order_count"],
                    "pending_count": payload["order_snapshot"]["summary"]["pending_count"],
                    "open_count": payload["order_snapshot"]["summary"]["open_count"],
                    "exception_count": payload["order_snapshot"]["summary"]["exception_count"],
                },
                "condition_orders": {
                    "count": payload["order_snapshot"]["condition_orders"]["count"],
                    "pending_count": payload["order_snapshot"]["condition_orders"]["pending_count"],
                    "open_count": payload["order_snapshot"]["condition_orders"]["open_count"],
                    "exception_count": payload["order_snapshot"]["condition_orders"]["exception_count"],
                    "status_counts": payload["order_snapshot"]["condition_orders"]["status_counts"],
                    "condition_type_counts": payload["order_snapshot"]["condition_orders"]["condition_type_counts"],
                    "sample": payload["order_snapshot"]["condition_orders"]["sample"],
                },
            },
            "signal_bus": {
                "version": payload["signal_bus"]["version"],
                "state": payload["signal_bus"]["state"],
                "market": {
                    "primary_code": payload["signal_bus"]["market"]["primary_code"],
                    "state": payload["signal_bus"]["market"]["state"],
                },
                "pool": {
                    "primary_code": payload["signal_bus"]["pool"]["primary_code"],
                    "state": payload["signal_bus"]["pool"]["state"],
                    "reason_codes": payload["signal_bus"]["pool"]["reason_codes"],
                },
                "trade": {
                    "primary_code": payload["signal_bus"]["trade"]["primary_code"],
                    "state": payload["signal_bus"]["trade"]["state"],
                    "reason_codes": payload["signal_bus"]["trade"]["reason_codes"],
                },
            },
            "pool_sync_state": {
                "status": payload["pool_sync_state"]["status"],
                "snapshot_date": payload["pool_sync_state"]["snapshot_date"],
            },
            "paper_trade_audit": {
                "status": payload["paper_trade_audit"]["status"],
                "event_only_codes": payload["paper_trade_audit"]["event_only_codes"],
                "broker_only_codes": payload["paper_trade_audit"]["broker_only_codes"],
            },
            "shadow_trade_state": {
                "status": payload["shadow_trade_state"]["status"],
                "positions_count": payload["shadow_trade_state"]["positions_count"],
                "automation_scope": payload["shadow_trade_state"]["automation_scope"],
                "advisory_summary": {
                    "triggered_signal_count": payload["shadow_trade_state"]["advisory_summary"]["triggered_signal_count"],
                    "triggered_rules": payload["shadow_trade_state"]["advisory_summary"]["triggered_rules"],
                },
            },
            "rule_automation_scope": [
                {
                    "name": item["name"],
                    "mode": item["mode"],
                }
                for item in payload["rule_automation_scope"]
            ],
            "pool_management": {
                "updated_at": payload["pool_management"]["updated_at"],
                "last_eval_date": payload["pool_management"]["last_eval_date"],
                "summary": payload["pool_management"]["summary"],
                "state_path": payload["pool_management"]["state_path"],
            },
        }

    def _state_orders_contract(self, payload: dict) -> dict:
        return {
            "command": payload["command"],
            "action": payload["action"],
            "status": payload["status"],
            "db_path": payload["db_path"],
            "scope_filter": payload["scope_filter"],
            "order_status_filter": payload["order_status_filter"],
            "summary": {
                "order_count": payload["summary"]["order_count"],
                "pending_count": payload["summary"]["pending_count"],
                "open_count": payload["summary"]["open_count"],
                "exception_count": payload["summary"]["exception_count"],
                "terminal_count": payload["summary"]["terminal_count"],
                "status_counts": payload["summary"]["status_counts"],
                "scope_counts": payload["summary"]["scope_counts"],
                "class_counts": payload["summary"]["class_counts"],
            },
            "condition_orders": {
                "count": payload["condition_orders"]["count"],
                "pending_count": payload["condition_orders"]["pending_count"],
                "open_count": payload["condition_orders"]["open_count"],
                "exception_count": payload["condition_orders"]["exception_count"],
                "status_counts": payload["condition_orders"]["status_counts"],
                "condition_type_counts": payload["condition_orders"]["condition_type_counts"],
                "sample": payload["condition_orders"]["sample"],
            },
        }

    def _run_for_orders(self, argv_variants: list[list[str]], patches: list[mock._patch], contract_fn, fixture_name: str):
        expected = _load_fixture(fixture_name)
        outputs = []
        for argv in argv_variants:
            payload = self._run_main(argv, patches)
            outputs.append(contract_fn(payload))
        for output in outputs:
            self.assertEqual(output, expected)
        self.assertEqual(outputs[0], outputs[1])

    def test_doctor_json_contract(self):
        import scripts.cli.trade as trade

        patches = [
            mock.patch.object(trade, "now_ts", side_effect=["2026-04-09T09:30:00", "2026-04-09T09:30:00"]),
            mock.patch.object(trade.sys, "executable", "/usr/bin/python3"),
            mock.patch.object(trade.platform, "python_version", return_value="3.11.0"),
            mock.patch.object(trade, "ObsidianVault", return_value=mock.Mock(vault_path=str(trade.PROJECT_ROOT))),
            mock.patch.object(trade, "get_notification", return_value={"discord": {"webhook_url": ""}}),
            mock.patch.object(trade, "_check_path_writable", return_value={"ok": True}),
            mock.patch.object(trade, "load_daily_state", return_value={
                "date": "2026-04-09",
                "pipelines": {"morning": {}},
            }),
            mock.patch.object(trade, "_combined_state_audit", return_value={
                "status": "drift",
                "snapshot_date": "2026-04-09",
                "checks": {"structured_ledger": {"ok": True}},
            }),
            mock.patch.object(trade, "_requests_ok", side_effect=lambda *_args, **_kwargs: {"ok": True}),
        ]

        self._run_for_orders(
            [["trade", "--json", "doctor"], ["trade", "doctor", "--json"]],
            patches,
            self._doctor_contract,
            "doctor.json",
        )

    def test_state_audit_json_contract(self):
        import scripts.cli.trade as trade

        patches = [
            mock.patch.object(trade, "LEDGER_DB_PATH", "/tmp/trade_state.sqlite3"),
            mock.patch.object(trade, "_combined_state_audit", return_value={
                "status": "drift",
                "snapshot_date": "2026-04-09",
                "checks": {
                    "stocks_yaml": {"ok": True},
                    "obsidian_projection": {"ok": True},
                    "paper_trade_consistency": {
                        "ok": False,
                        "status": "drift",
                        "event_only_codes": ["300389"],
                        "broker_only_codes": [],
                    },
                },
            }),
        ]

        self._run_for_orders(
            [["trade", "--json", "state", "audit"], ["trade", "state", "audit", "--json"]],
            patches,
            self._state_audit_contract,
            "state_audit.json",
        )

    def test_state_reconcile_json_contract_dry_run(self):
        import scripts.cli.trade as trade

        patches = [
            mock.patch.object(trade, "LEDGER_DB_PATH", "/tmp/trade_state.sqlite3"),
            mock.patch("scripts.pipeline.shadow_trade.reconcile_trade_state", return_value={
                "status": "drift",
                "apply": False,
                "planned_action_count": 2,
                "planned_actions": [
                    {
                        "action": "flatten_missing_broker_position",
                        "side": "sell",
                        "code": "300389",
                        "shares": 1000,
                        "reason_code": "PAPER_RECONCILE_FLATTEN",
                    },
                    {
                        "action": "open_missing_event_position",
                        "side": "buy",
                        "code": "603063",
                        "shares": 600,
                        "reason_code": "PAPER_RECONCILE_OPEN",
                    },
                ],
                "applied_actions": [],
                "consistency_before": {
                    "status": "drift",
                    "event_only_codes": ["300389"],
                    "broker_only_codes": ["603063"],
                },
                "consistency_after": {
                    "status": "drift",
                    "event_only_codes": ["300389"],
                    "broker_only_codes": ["603063"],
                },
            }),
        ]

        self._run_for_orders(
            [["trade", "--json", "state", "reconcile"], ["trade", "state", "reconcile", "--json"]],
            patches,
            self._state_reconcile_contract,
            "state_reconcile_dry_run.json",
        )

    def test_state_reconcile_json_contract_apply(self):
        import scripts.cli.trade as trade

        patches = [
            mock.patch.object(trade, "LEDGER_DB_PATH", "/tmp/trade_state.sqlite3"),
            mock.patch("scripts.pipeline.shadow_trade.reconcile_trade_state", return_value={
                "status": "ok",
                "apply": True,
                "planned_action_count": 1,
                "planned_actions": [
                    {
                        "action": "flatten_missing_broker_position",
                        "side": "sell",
                        "code": "300389",
                        "shares": 1000,
                        "reason_code": "PAPER_RECONCILE_FLATTEN",
                    }
                ],
                "applied_actions": [
                    {
                        "action": "flatten_missing_broker_position",
                        "side": "sell",
                        "code": "300389",
                        "shares": 1000,
                        "reason_code": "PAPER_RECONCILE_FLATTEN",
                    }
                ],
                "consistency_before": {
                    "status": "drift",
                    "event_only_codes": ["300389"],
                    "broker_only_codes": [],
                },
                "consistency_after": {
                    "status": "ok",
                    "event_only_codes": [],
                    "broker_only_codes": [],
                },
            }),
        ]

        self._run_for_orders(
            [["trade", "--json", "state", "reconcile", "--apply"], ["trade", "state", "reconcile", "--apply", "--json"]],
            patches,
            self._state_reconcile_contract,
            "state_reconcile_apply.json",
        )

    def test_status_today_json_contract(self):
        import scripts.cli.trade as trade

        patches = [
            mock.patch.object(trade, "LEDGER_DB_PATH", "/tmp/trade_state.sqlite3"),
            mock.patch.object(trade, "_preflight_state_sync", return_value={"status": "success", "target": "all", "steps": []}),
            mock.patch.object(trade, "load_daily_state", return_value={
                "date": "2026-04-09",
                "updated_at": "2026-04-09T09:00:00",
                "pipelines": {
                    "morning": {
                        "status": "success",
                        "updated_at": "2026-04-09T08:30:00",
                    }
                },
            }),
            mock.patch.object(trade, "get_strategy", return_value={}),
            mock.patch.object(trade, "build_today_decision", return_value={
                "action": "CLEAR",
                "weekly_buys": 0,
            }),
            mock.patch.object(trade, "load_portfolio_snapshot", return_value={
                "summary": {
                    "holding_count": 0,
                    "current_exposure": 0.0,
                },
            }),
            mock.patch.object(trade, "load_pool_snapshot", return_value={
                "updated_at": "2026-04-09T09:25:00",
                "snapshot_date": "2026-04-09",
                "summary": {
                    "core_count": 2,
                    "watch_count": 5,
                    "other_count": 0,
                },
                "entries": [
                    {
                        "code": "300389",
                        "name": "艾比森",
                        "bucket": "core",
                        "veto_triggered": False,
                        "veto_signals": ["consecutive_outflow_warn"],
                    }
                ],
            }),
            mock.patch.object(trade, "audit_state", return_value={
                "status": "ok",
                "snapshot_date": "2026-04-09",
                "checks": {"stocks_yaml": {"ok": True}},
            }),
            mock.patch.object(trade, "load_market_snapshot", return_value={
                "signal": "CLEAR",
                "source": "market_timer",
                "source_chain": ["market_timer"],
                "as_of_date": "2026-04-09",
            }),
            mock.patch.object(trade, "load_order_snapshot", return_value={
                "scope": "paper_mx",
                "status": "all",
                "db_path": "/tmp/trade_state.sqlite3",
                "orders": [
                    {
                        "external_id": "paper:order:001",
                        "scope": "paper_mx",
                        "order_class": "condition",
                        "condition_type": "take_profit_t1",
                        "code": "300389",
                        "name": "艾比森",
                        "side": "sell",
                        "status": "candidate",
                        "requested_shares": 1000,
                        "filled_shares": 0,
                        "trigger_price": 20.5,
                        "limit_price": 20.45,
                        "confirm_status": "pending",
                    },
                    {
                        "external_id": "paper:order:002",
                        "scope": "paper_mx",
                        "order_class": "condition",
                        "condition_type": "manual_stop",
                        "code": "603063",
                        "name": "禾望电气",
                        "side": "sell",
                        "status": "placed",
                        "requested_shares": 600,
                        "filled_shares": 0,
                        "trigger_price": 18.2,
                        "limit_price": 18.1,
                        "confirm_status": "not_required",
                    },
                    {
                        "external_id": "paper:order:003",
                        "scope": "paper_mx",
                        "order_class": "manual",
                        "condition_type": "",
                        "code": "000001",
                        "name": "上证指数",
                        "side": "buy",
                        "status": "exception",
                        "requested_shares": 0,
                        "filled_shares": 0,
                        "trigger_price": 0.0,
                        "limit_price": 0.0,
                        "confirm_status": "not_required",
                    },
                    {
                        "external_id": "paper:order:004",
                        "scope": "paper_mx",
                        "order_class": "manual",
                        "condition_type": "",
                        "code": "300750",
                        "name": "宁德时代",
                        "side": "buy",
                        "status": "filled",
                        "requested_shares": 100,
                        "filled_shares": 100,
                        "trigger_price": 0.0,
                        "limit_price": 0.0,
                        "confirm_status": "not_required",
                    },
                ],
                "summary": {
                    "order_count": 4,
                    "open_count": 2,
                    "terminal_count": 2,
                    "status_counts": {
                        "candidate": 1,
                        "placed": 1,
                        "exception": 1,
                        "filled": 1,
                    },
                    "scope_counts": {
                        "paper_mx": 4,
                    },
                    "class_counts": {
                        "condition": 2,
                        "manual": 2,
                    },
                },
            }),
            mock.patch.object(trade, "_shadow_trade_snapshot", return_value={
                "status": "drift",
                "timestamp": "2026-04-09 10:00",
                "positions_count": 1,
                "automation_scope": "本波仅自动执行：动态止损、绝对止损、第一批止盈；时间止损与回撤止盈仅作为提示，不自动下单。",
                "advisory_summary": {
                    "triggered_signal_count": 2,
                    "triggered_rules": [
                        "RISK_TIME_STOP",
                        "RISK_DRAWDOWN_TAKE_PROFIT",
                    ],
                },
                "consistency": {
                    "status": "drift",
                    "event_only_codes": ["300389"],
                    "broker_only_codes": [],
                },
            }),
        ]

        self._run_for_orders(
            [["trade", "--json", "status", "today"], ["trade", "status", "today", "--json"]],
            patches,
            self._status_today_contract,
            "status_today.json",
        )

    def test_state_orders_json_contract(self):
        import scripts.cli.trade as trade

        patches = [
            mock.patch.object(trade, "LEDGER_DB_PATH", "/tmp/trade_state.sqlite3"),
            mock.patch.object(trade, "load_order_snapshot", return_value={
                "scope": "all",
                "status": "all",
                "db_path": "/tmp/trade_state.sqlite3",
                "orders": [
                    {
                        "external_id": "paper:order:001",
                        "scope": "paper_mx",
                        "order_class": "condition",
                        "condition_type": "take_profit_t1",
                        "code": "300389",
                        "name": "艾比森",
                        "side": "sell",
                        "status": "candidate",
                        "requested_shares": 1000,
                        "filled_shares": 0,
                        "trigger_price": 20.5,
                        "limit_price": 20.45,
                        "confirm_status": "pending",
                    },
                    {
                        "external_id": "cn:order:002",
                        "scope": "cn_a_system",
                        "order_class": "manual",
                        "condition_type": "",
                        "code": "603063",
                        "name": "禾望电气",
                        "side": "buy",
                        "status": "filled",
                        "requested_shares": 600,
                        "filled_shares": 600,
                        "trigger_price": 0.0,
                        "limit_price": 0.0,
                        "confirm_status": "not_required",
                    },
                ],
                "summary": {
                    "order_count": 2,
                    "open_count": 1,
                    "terminal_count": 1,
                    "status_counts": {
                        "candidate": 1,
                        "filled": 1,
                    },
                    "scope_counts": {
                        "paper_mx": 1,
                        "cn_a_system": 1,
                    },
                    "class_counts": {
                        "condition": 1,
                        "manual": 1,
                    },
                },
            }),
        ]

        self._run_for_orders(
            [["trade", "--json", "state", "orders"], ["trade", "state", "orders", "--json"]],
            patches,
            self._state_orders_contract,
            "state_orders.json",
        )


if __name__ == "__main__":
    unittest.main()
