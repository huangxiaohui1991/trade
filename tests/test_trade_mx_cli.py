import contextlib
import io
import json
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


if __name__ == "__main__":
    unittest.main()
