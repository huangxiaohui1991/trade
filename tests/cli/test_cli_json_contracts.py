import os
import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts.utils.config_loader import clear_config_cache


FIXTURE_DIR = Path(__file__).resolve().parent.parent / "fixtures" / "cli_contracts"


def _load_fixture(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


class CLIJsonContractTests(unittest.TestCase):
    def setUp(self):
        clear_config_cache()
        self._old_db_path = os.environ.get("TRADE_STATE_DB_PATH")
        self._old_discord_vars = {
            k: os.environ.pop(k, None)
            for k in ("DISCORD_WEBHOOK_URL", "DISCORD_BOT_TOKEN", "DISCORD_CHANNEL_ID", "DISCORD_DM_USER_ID")
        }
        self._tmpdir = tempfile.TemporaryDirectory()
        os.environ["TRADE_STATE_DB_PATH"] = str(Path(self._tmpdir.name) / "trade_state.sqlite3")

    def tearDown(self):
        clear_config_cache()
        if self._old_db_path is None:
            os.environ.pop("TRADE_STATE_DB_PATH", None)
        else:
            os.environ["TRADE_STATE_DB_PATH"] = self._old_db_path
        for k, v in self._old_discord_vars.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        self._tmpdir.cleanup()

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

    def _alert_items_contract(self, alerts: list[dict]) -> list[dict]:
        def _normalize_details(details: dict) -> dict:
            if not isinstance(details, dict):
                return details
            normalized = dict(details)
            condition_orders = normalized.get("condition_orders")
            if isinstance(condition_orders, dict):
                normalized["condition_orders"] = {
                    "count": condition_orders.get("count"),
                    "pending_count": condition_orders.get("pending_count"),
                    "open_count": condition_orders.get("open_count"),
                    "exception_count": condition_orders.get("exception_count"),
                    "status_counts": condition_orders.get("status_counts"),
                    "condition_type_counts": condition_orders.get("condition_type_counts"),
                    "sample": condition_orders.get("sample"),
                }
            return normalized

        items = [
            {
                "level": item["level"],
                "code": item["code"],
                "summary": item["summary"],
                "details": _normalize_details(item["details"]),
                "acknowledged": item["acknowledged"],
                "acknowledged_at": item["acknowledged_at"],
                "acknowledged_by": item["acknowledged_by"],
            }
            for item in alerts
        ]
        return sorted(items, key=lambda item: (item["code"], item["summary"], item["level"]))

    def _ack_summary_contract(self, ack_summary: dict) -> dict:
        return {
            "acknowledged_count": ack_summary["acknowledged_count"],
            "pending_count": ack_summary["pending_count"],
            "all_acknowledged": ack_summary["all_acknowledged"],
        }

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
                    "mode": checks["discord_webhook"]["mode"],
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
                "data_source_health": {
                    "status": checks["data_source_health"]["status"],
                    "warning": checks["data_source_health"]["warning"],
                    "recent_runs": {
                        "run_count": checks["data_source_health"]["recent_runs"]["run_count"],
                        "usable_rate": checks["data_source_health"]["recent_runs"]["usable_rate"],
                    },
                    "cache_summary": {
                        "file_count": checks["data_source_health"]["cache_summary"]["file_count"],
                        "warning_namespace_count": checks["data_source_health"]["cache_summary"]["warning_namespace_count"],
                    },
                    "score_data_quality": {
                        "entry_count": checks["data_source_health"]["score_data_quality"]["entry_count"],
                        "missing_field_rate": checks["data_source_health"]["score_data_quality"]["missing_field_rate"],
                    },
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
            "alert_snapshot": {
                "status": payload["alert_snapshot"]["status"],
                "snapshot_date": payload["alert_snapshot"]["snapshot_date"],
                "alert_count": payload["alert_snapshot"]["alert_count"],
                "status_summary": {
                    "status": payload["alert_snapshot"]["status_summary"]["status"],
                    "alert_count": payload["alert_snapshot"]["status_summary"]["alert_count"],
                    "level_counts": payload["alert_snapshot"]["status_summary"]["level_counts"],
                    "code_counts": payload["alert_snapshot"]["status_summary"]["code_counts"],
                    "ack_summary": self._ack_summary_contract(payload["alert_snapshot"]["status_summary"]["ack_summary"]),
                    "snapshot_date": payload["alert_snapshot"]["status_summary"]["snapshot_date"],
                },
                "classification": {
                    "by_level": payload["alert_snapshot"]["classification"]["by_level"],
                    "by_code": payload["alert_snapshot"]["classification"]["by_code"],
                    "by_level_code": payload["alert_snapshot"]["classification"]["by_level_code"],
                },
                "ack_summary": self._ack_summary_contract(payload["alert_snapshot"]["ack_summary"]),
                "signal_bus_state": payload["alert_snapshot"]["signal_bus_state"],
                "pool_snapshot_date": payload["alert_snapshot"]["pool_snapshot_date"],
                "market_signal": payload["alert_snapshot"]["market_signal"],
                "alerts": self._alert_items_contract(payload["alert_snapshot"]["alerts"]),
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

    def _state_confirm_contract(self, payload: dict) -> dict:
        return {
            "command": payload["command"],
            "action": payload["action"],
            "status": payload["status"],
            "db_path": payload["db_path"],
            "scope_filter": payload["scope_filter"],
            "matched_order_count": payload["matched_order_count"],
            "created_order": payload["created_order"],
            "trade_event_recorded": payload["trade_event_recorded"],
            "reply": {
                "action": payload["reply"]["action"],
                "type": payload["reply"]["type"],
                "stock": payload["reply"]["stock"],
                "price": payload["reply"]["price"],
                "filled_price": payload["reply"]["filled_price"],
                "raw": payload["reply"]["raw"],
            },
            "order": {
                "external_id": payload["order"]["external_id"],
                "status": payload["order"]["status"],
                "confirm_status": payload["order"]["confirm_status"],
                "avg_fill_price": payload["order"]["avg_fill_price"],
            },
        }

    def _state_remind_contract(self, payload: dict) -> dict:
        return {
            "command": payload["command"],
            "action": payload["action"],
            "status": payload["status"],
            "db_path": payload["db_path"],
            "scope_filter": payload["scope_filter"],
            "pending_count": payload["pending_count"],
            "send": payload["send"],
            "discord_ok": payload["discord_ok"],
            "discord_error": payload["discord_error"],
            "pending": payload["pending"],
            "content": payload["content"],
        }

    def _state_trade_review_contract(self, payload: dict) -> dict:
        return {
            "command": payload["command"],
            "action": payload["action"],
            "status": payload["status"],
            "db_path": payload["db_path"],
            "scope": payload["scope"],
            "window": payload["window"],
            "closed_trade_count": payload["closed_trade_count"],
            "win_count": payload["win_count"],
            "loss_count": payload["loss_count"],
            "win_rate": payload["win_rate"],
            "total_realized_pnl": payload["total_realized_pnl"],
            "mfe_mae_status": payload["mfe_mae_status"],
            "closed_trades": payload["closed_trades"],
        }

    def _state_alerts_contract(self, payload: dict) -> dict:
        return {
            "command": payload["command"],
            "action": payload["action"],
            "status": payload["status"],
            "db_path": payload["db_path"],
            "alert_count": payload["alert_count"],
            "snapshot_date": payload["snapshot_date"],
            "signal_bus_state": payload["signal_bus_state"],
            "pool_snapshot_date": payload["pool_snapshot_date"],
            "status_summary": {
                "status": payload["status_summary"]["status"],
                "alert_count": payload["status_summary"]["alert_count"],
                "level_counts": payload["status_summary"]["level_counts"],
                "code_counts": payload["status_summary"]["code_counts"],
                    "ack_summary": self._ack_summary_contract(payload["status_summary"]["ack_summary"]),
                "snapshot_date": payload["status_summary"]["snapshot_date"],
            },
            "classification": {
                "by_level": payload["classification"]["by_level"],
                "by_code": payload["classification"]["by_code"],
                "by_level_code": payload["classification"]["by_level_code"],
            },
            "ack_summary": self._ack_summary_contract(payload["ack_summary"]),
            "alerts": self._alert_items_contract(payload["alerts"]),
        }

    def _run_for_orders(self, argv_variants: list[list[str]], patches: list[mock._patch], contract_fn, fixture_name: str):
        expected = _load_fixture(fixture_name)
        def _normalize_expected_alerts(value):
            if isinstance(value, dict):
                for key, item in list(value.items()):
                    if key == "alerts" and isinstance(item, list):
                        value[key] = sorted(item, key=lambda alert: (alert["code"], alert["summary"], alert["level"]))
                    else:
                        _normalize_expected_alerts(item)
            elif isinstance(value, list):
                for item in value:
                    _normalize_expected_alerts(item)
        _normalize_expected_alerts(expected)
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
            mock.patch.object(trade, "_data_source_health_snapshot", return_value={
                "ok": True,
                "status": "ok",
                "warning": [],
                "recent_runs": {"run_count": 2, "usable_rate": 1.0},
                "cache_summary": {"file_count": 4, "warning_namespace_count": 0},
                "score_data_quality": {"entry_count": 2, "missing_field_rate": 0.0},
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
            mock.patch("scripts.state.service._now_ts", return_value="2026-04-09T09:30:00"),
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

    def test_status_today_includes_mx_health(self):
        import scripts.cli.trade as trade

        patches = [
            mock.patch.object(trade, "_preflight_state_sync", return_value={"status": "success", "target": "all", "steps": []}),
            mock.patch.object(trade, "load_daily_state", return_value={"date": "2026-04-09", "updated_at": "2026-04-09T09:00:00", "pipelines": {}}),
            mock.patch.object(trade, "get_strategy", return_value={}),
            mock.patch.object(trade, "build_today_decision", return_value={"action": "CLEAR", "weekly_buys": 0}),
            mock.patch.object(trade, "load_portfolio_snapshot", return_value={"summary": {"holding_count": 0, "current_exposure": 0.0}}),
            mock.patch.object(trade, "load_pool_snapshot", return_value={"updated_at": "", "snapshot_date": "2026-04-09", "summary": {}, "entries": []}),
            mock.patch.object(trade, "audit_state", return_value={"status": "ok", "snapshot_date": "2026-04-09", "checks": {}}),
            mock.patch.object(trade, "load_market_snapshot", return_value={"signal": "CLEAR", "source": "market_timer", "source_chain": [], "as_of_date": "2026-04-09"}),
            mock.patch.object(trade, "_mx_health_snapshot", return_value={
                "status": "warning",
                "available_count": 3,
                "unavailable_count": 1,
                "command_count": 4,
                "group_count": 3,
                "groups": {"data": 1, "search": 1, "moni": 2},
                "required": {},
                "unavailable_commands": ["mx.moni.buy"],
                "source": "scripts.mx.cli_tools",
            }),
            mock.patch.object(trade, "_shadow_trade_snapshot", return_value={
                "status": "ok",
                "timestamp": "2026-04-09 10:00",
                "positions_count": 0,
                "automation_scope": "",
                "advisory_summary": {"triggered_signal_count": 0, "triggered_rules": []},
                "mx_health": {
                    "status": "ok",
                    "available_count": 4,
                    "unavailable_count": 0,
                    "command_count": 4,
                    "group_count": 3,
                },
                "consistency": {"ok": True, "status": "ok", "event_only_codes": [], "broker_only_codes": []},
            }),
            mock.patch.object(trade, "load_order_snapshot", return_value={
                "scope": "paper_mx",
                "status": "all",
                "db_path": "/tmp/trade_state.sqlite3",
                "orders": [],
                "summary": {"order_count": 0, "open_count": 0, "terminal_count": 0, "status_counts": {}, "scope_counts": {}, "class_counts": {}, "pending_count": 0, "exception_count": 0},
            }),
            mock.patch.object(trade, "build_signal_bus_summary", return_value={"state": "ok"}),
        ]

        payload = self._run_main(["trade", "--json", "status", "today"], patches)
        self.assertEqual(payload["mx_health"]["status"], "warning")
        self.assertEqual(payload["shadow_trade_state"]["mx_health"]["status"], "ok")

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

    def test_state_confirm_json_contract(self):
        import scripts.cli.trade as trade

        patches = [
            mock.patch.object(trade, "LEDGER_DB_PATH", "/tmp/trade_state.sqlite3"),
            mock.patch.object(trade, "apply_order_reply", return_value={
                "status": "ok",
                "reply": {
                    "action": "触发",
                    "type": "止盈",
                    "stock": "艾比森",
                    "price": None,
                    "filled_price": 21.3,
                    "raw": "止盈触发了 艾比森 成交¥21.3",
                },
                "created_order": False,
                "matched_order_count": 1,
                "trade_event_recorded": True,
                "order": {
                    "external_id": "paper:order:001",
                    "status": "filled",
                    "confirm_status": "confirmed",
                    "avg_fill_price": 21.3,
                },
                "db_path": "/tmp/trade_state.sqlite3",
            }),
        ]

        self._run_for_orders(
            [
                ["trade", "--json", "state", "confirm", "--reply", "止盈触发了 艾比森 成交¥21.3"],
                ["trade", "state", "confirm", "--reply", "止盈触发了 艾比森 成交¥21.3", "--json"],
            ],
            patches,
            self._state_confirm_contract,
            "state_confirm.json",
        )

    def test_state_remind_json_contract(self):
        import scripts.cli.trade as trade

        patches = [
            mock.patch.object(trade, "LEDGER_DB_PATH", "/tmp/trade_state.sqlite3"),
            mock.patch.object(trade, "pending_condition_order_items", return_value=[
                {
                    "external_id": "paper:order:001",
                    "name": "艾比森",
                    "code": "300389",
                    "type": "止损",
                    "price": 20.3,
                    "currency": "¥",
                    "status": "pending",
                }
            ]),
            mock.patch.object(
                trade,
                "render_condition_order_reminder",
                return_value="提醒内容",
            ),
        ]

        self._run_for_orders(
            [["trade", "--json", "state", "remind"], ["trade", "state", "remind", "--json"]],
            patches,
            self._state_remind_contract,
            "state_remind.json",
        )

    def test_state_trade_review_json_contract(self):
        import scripts.cli.trade as trade

        patches = [
            mock.patch.object(trade, "LEDGER_DB_PATH", "/tmp/trade_state.sqlite3"),
            mock.patch.object(trade, "load_trade_review", return_value={
                "scope": "cn_a_system",
                "window": 90,
                "closed_trade_count": 1,
                "win_count": 1,
                "loss_count": 0,
                "win_rate": 100.0,
                "total_realized_pnl": 2200.0,
                "open_position_count": 0,
                "closed_trades": [
                    {
                        "code": "300389",
                        "name": "艾比森",
                        "entry_date": "2026-04-07",
                        "exit_date": "2026-04-09",
                        "holding_days": 2,
                        "entry_price": 19.1,
                        "exit_price": 21.3,
                        "buy_count": 1,
                        "sell_count": 1,
                        "entry_reason_code": "BUY_CORE_POOL",
                        "entry_reason_codes": ["BUY_CORE_POOL"],
                        "entry_reason_text": "核心池评分7.6",
                        "exit_reason_codes": ["RISK_TAKE_PROFIT_T1"],
                        "exit_reason_texts": ["第一批止盈"],
                        "realized_pnl": 2200.0,
                        "rule_tags": ["entry", "risk"],
                        "mfe_pct": None,
                        "mae_pct": None,
                    }
                ],
                "open_positions": [],
                "source": "structured_ledger",
                "mfe_mae_status": "pending_market_history",
            }),
        ]

        self._run_for_orders(
            [["trade", "--json", "state", "trade-review"], ["trade", "state", "trade-review", "--json"]],
            patches,
            self._state_trade_review_contract,
            "state_trade_review.json",
        )

    def test_state_alerts_json_contract(self):
        import scripts.cli.trade as trade

        patches = [
            mock.patch.object(trade, "LEDGER_DB_PATH", "/tmp/trade_state.sqlite3"),
            mock.patch.object(trade, "get_strategy", return_value={}),
            mock.patch("scripts.state.service._now_ts", return_value="2026-04-09T09:30:00"),
            mock.patch.object(trade, "build_today_decision", return_value={
                "decision": "NO_TRADE",
                "market_signal": "CLEAR",
                "portfolio_risk": {
                    "state": "block",
                    "reason_codes": ["TRADE_CONSECUTIVE_LOSS_COOLDOWN"],
                    "reasons": ["连续亏损冷却中"],
                },
            }),
            mock.patch.object(trade, "load_pool_snapshot", return_value={"snapshot_date": "2026-04-09"}),
            mock.patch.object(trade, "audit_state", return_value={"status": "drift", "snapshot_date": "2026-04-09"}),
            mock.patch.object(trade, "load_market_snapshot", return_value={"signal": "CLEAR"}),
            mock.patch.object(trade, "_shadow_trade_snapshot", return_value={
                "consistency": {
                    "ok": False,
                    "status": "drift",
                    "event_only_codes": ["300389"],
                    "broker_only_codes": [],
                },
                "advisory_summary": {
                    "triggered_signal_count": 1,
                    "triggered_position_count": 1,
                    "triggered_rules": ["RISK_TIME_STOP"],
                },
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
                        "condition_type": "manual_stop",
                        "code": "300389",
                        "name": "艾比森",
                        "side": "sell",
                        "status": "candidate",
                        "requested_shares": 1000,
                        "filled_shares": 0,
                        "trigger_price": 20.5,
                        "limit_price": 20.45,
                        "confirm_status": "pending",
                    }
                ],
                "summary": {
                    "order_count": 1,
                    "open_count": 0,
                    "terminal_count": 0,
                    "status_counts": {"candidate": 1},
                    "scope_counts": {"paper_mx": 1},
                    "class_counts": {"condition": 1},
                    "pending_count": 1,
                    "exception_count": 0,
                },
            }),
            mock.patch.object(trade, "build_signal_bus_summary", return_value={"state": "drift"}),
        ]

        self._run_for_orders(
            [["trade", "--json", "state", "alerts"], ["trade", "state", "alerts", "--json"]],
            patches,
            self._state_alerts_contract,
            "state_alerts.json",
        )

    def test_backtest_validate_single_json_contract(self):
        params_path = Path(self._tmpdir.name) / "validate_single_params.json"
        params_path.write_text(
            json.dumps({
                "entry_mode": "trend_follow",
                "score_threshold": 68,
            }),
            encoding="utf-8",
        )
        output_path = Path(self._tmpdir.name) / "validate_single_report.json"
        fake_result = {
            "command": "backtest",
            "action": "single_stock_strategy_validation",
            "status": "ok",
            "stock_code": "601869",
            "start": "2025-04-11",
            "end": "2026-04-10",
            "index_code": "system",
            "performance": {
                "closed_trade_count": 8,
                "total_realized_pnl": 110580.02,
                "max_drawdown_pct": -3.12,
            },
            "diagnostics": {
                "signal_statistics": {
                    "market_positive_days": 163,
                    "score_ready_days": 46,
                    "buy_ready_days": 12,
                    "actual_entry_days": 8,
                },
                "opportunity_statistics": {
                    "total_opportunity_windows": 7,
                    "captured_opportunity_windows": 4,
                    "missed_opportunity_windows": 3,
                    "capture_rate_pct": 57.1,
                    "weighted_capture_rate_pct": 85.1,
                },
                "findings": [
                    "存在 6 次提前离场，最大卖飞发生在 2025-08-08 后少赚 65.8%",
                ],
            },
            "opportunity_windows": [],
            "premature_exits": [],
            "closed_trades": [],
            "open_positions": [],
            "rejected_entries": [],
        }

        with mock.patch(
            "scripts.backtest.historical_pipeline.run_single_stock_strategy_validation",
            return_value=dict(fake_result),
        ) as run_validation:
            payload = self._run_main(
                [
                    "trade",
                    "--json",
                    "backtest",
                    "validate-single",
                    "--code",
                    "601869",
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
                    "--opportunity-lookahead-days",
                    "30",
                    "--opportunity-min-gain-pct",
                    "0.2",
                    "--premature-exit-min-gain-pct",
                    "0.1",
                    "--output",
                    str(output_path),
                ],
                [],
            )

        run_validation.assert_called_once_with(
            stock_code="601869",
            start="2025-04-11",
            end="2026-04-10",
            index_code="system",
            total_capital=500000.0,
            strategy_params={
                "entry_mode": "trend_follow",
                "score_threshold": 68,
                "preset": "aggressive_high_return",
            },
            opportunity_lookahead_days=30,
            opportunity_min_gain_pct=0.2,
            premature_exit_min_gain_pct=0.1,
        )
        self.assertEqual(payload["command"], "backtest")
        self.assertEqual(payload["action"], "single_stock_strategy_validation")
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["stock_code"], "601869")
        self.assertEqual(payload["report_path"], str(output_path))
        self.assertEqual(payload["performance"]["closed_trade_count"], 8)
        self.assertEqual(payload["diagnostics"]["opportunity_statistics"]["capture_rate_pct"], 57.1)
        self.assertEqual(json.loads(output_path.read_text(encoding="utf-8")), fake_result)


if __name__ == "__main__":
    unittest.main()
