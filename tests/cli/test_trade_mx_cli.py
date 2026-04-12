import contextlib
import io
import json
import os
import unittest
from unittest import mock


class TradeMxCliTests(unittest.TestCase):
    def test_mx_list_command_dispatches(self):
        import scripts.cli.trade as trade

        payload = {
            "command": "mx",
            "action": "list",
            "status": "ok",
            "item_count": 2,
            "items": [{"id": "mx.data.query"}, {"id": "mx.search.news"}],
        }
        stdout = io.StringIO()
        with mock.patch.object(trade, "mx_command", return_value=payload) as mx_mock, mock.patch.object(
            trade.sys,
            "argv",
            ["trade", "--json", "mx", "list"],
        ):
            with contextlib.redirect_stdout(stdout):
                trade.main()

        result = json.loads(stdout.getvalue())
        self.assertEqual(result["command"], "mx")
        self.assertEqual(result["action"], "list")
        mx_mock.assert_called_once()

    def test_mx_run_command_dispatches_with_arguments(self):
        import scripts.cli.trade as trade

        payload = {
            "command": "mx",
            "action": "run",
            "status": "ok",
            "mx_command": "mx.moni.buy",
            "arguments": {"stock_code": "600519", "quantity": 100},
            "result": {"ok": True},
        }
        stdout = io.StringIO()
        with mock.patch.object(trade, "mx_command", return_value=payload) as mx_mock, mock.patch.object(
            trade.sys,
            "argv",
            [
                "trade",
                "--json",
                "mx",
                "run",
                "mx.moni.buy",
                "--stock-code",
                "600519",
                "--quantity",
                "100",
            ],
        ):
            with contextlib.redirect_stdout(stdout):
                trade.main()

        result = json.loads(stdout.getvalue())
        self.assertEqual(result["command"], "mx")
        self.assertEqual(result["action"], "run")
        mx_mock.assert_called_once()

    def test_mx_health_command_dispatches(self):
        import scripts.cli.trade as trade

        payload = {
            "command": "mx",
            "action": "health",
            "status": "warning",
            "health": {
                "status": "warning",
                "available_count": 4,
                "unavailable_count": 1,
                "command_count": 5,
            },
        }
        stdout = io.StringIO()
        with mock.patch.object(trade, "mx_command", return_value=payload) as mx_mock, mock.patch.object(
            trade.sys,
            "argv",
            ["trade", "--json", "mx", "health"],
        ):
            with contextlib.redirect_stdout(stdout):
                trade.main()

        result = json.loads(stdout.getvalue())
        self.assertEqual(result["command"], "mx")
        self.assertEqual(result["action"], "health")
        self.assertEqual(result["health"]["available_count"], 4)
        mx_mock.assert_called_once()

    def test_doctor_accepts_discord_bot_mode(self):
        import scripts.cli.trade as trade

        old_webhook = os.environ.get("DISCORD_WEBHOOK_URL")
        old_bot_token = os.environ.get("DISCORD_BOT_TOKEN")
        old_channel_id = os.environ.get("DISCORD_CHANNEL_ID")
        old_dm_user_id = os.environ.get("DISCORD_DM_USER_ID")
        os.environ.pop("DISCORD_WEBHOOK_URL", None)
        os.environ["DISCORD_BOT_TOKEN"] = "bot-token"
        os.environ["DISCORD_CHANNEL_ID"] = "channel-id"
        os.environ.pop("DISCORD_DM_USER_ID", None)
        try:
            with mock.patch.object(trade, "ObsidianVault", return_value=mock.Mock(vault_path=str(trade.PROJECT_ROOT))), mock.patch.object(
                trade, "_check_path_writable", return_value={"ok": True}
            ), mock.patch.object(
                trade, "load_daily_state", return_value={"date": "2026-04-12", "pipelines": {}}
            ), mock.patch.object(
                trade, "_combined_state_audit", return_value={"status": "ok", "snapshot_date": "2026-04-12", "checks": {}}
            ), mock.patch.object(
                trade, "_data_source_health_snapshot", return_value={"ok": True, "status": "ok", "warning": []}
            ), mock.patch.object(
                trade, "_requests_ok", return_value={"ok": True}
            ), mock.patch.object(
                trade, "get_notification", return_value={"discord": {"webhook_url": "", "bot_token": "", "channel_id": ""}}
            ):
                payload = trade.doctor()
        finally:
            if old_webhook is None:
                os.environ.pop("DISCORD_WEBHOOK_URL", None)
            else:
                os.environ["DISCORD_WEBHOOK_URL"] = old_webhook
            if old_bot_token is None:
                os.environ.pop("DISCORD_BOT_TOKEN", None)
            else:
                os.environ["DISCORD_BOT_TOKEN"] = old_bot_token
            if old_channel_id is None:
                os.environ.pop("DISCORD_CHANNEL_ID", None)
            else:
                os.environ["DISCORD_CHANNEL_ID"] = old_channel_id
            if old_dm_user_id is None:
                os.environ.pop("DISCORD_DM_USER_ID", None)
            else:
                os.environ["DISCORD_DM_USER_ID"] = old_dm_user_id

        self.assertTrue(payload["checks"]["discord_webhook"]["configured"])
        self.assertEqual(payload["checks"]["discord_webhook"]["mode"], "bot_channel")


if __name__ == "__main__":
    unittest.main()
