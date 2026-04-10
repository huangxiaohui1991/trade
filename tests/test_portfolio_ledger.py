import os
import tempfile
import unittest
from unittest import mock


class PortfolioLedgerTests(unittest.TestCase):
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

    def test_load_portfolio_snapshot_paper_mx_uses_broker_positions_and_preserves_other_scopes(self):
        from scripts.state import bootstrap_state, load_portfolio_snapshot

        bootstrap_state(force=True)

        broker = mock.Mock()
        broker.positions.return_value = {
            "code": "200",
            "data": {
                "posList": [
                    {
                        "stockCode": "300389",
                        "stockName": "艾比森",
                        "totalQty": 1000,
                        "costPrice": 19.13,
                        "lastPrice": 19.50,
                        "marketValue": 19500,
                        "status": "持仓中",
                    },
                    {
                        "secuCode": "603063",
                        "secuName": "禾望电气",
                        "currentQty": 600,
                        "avgCost": 32.76,
                        "currentPrice": 33.10,
                    },
                ]
            },
        }
        broker.balance.return_value = {
            "code": "200",
            "data": {
                "totalAssets": 350000,
                "availBalance": 125000,
                "totalPosValue": 225000,
                "totalProfit": 5000,
                "initMoney": 200000,
            },
        }

        with mock.patch("scripts.state.service.MXMoni", return_value=broker):
            snapshot = load_portfolio_snapshot(scope="paper_mx")

        self.assertEqual(snapshot["scope"], "paper_mx")
        self.assertEqual(snapshot["summary"]["holding_count"], 2)
        self.assertEqual(snapshot["summary"]["cash_value"], 125000.0)
        self.assertEqual(snapshot["summary"]["total_capital"], 350000.0)
        self.assertEqual(snapshot["summary"]["current_exposure"], 0.643)

        positions = {item["code"]: item for item in snapshot["positions"]}
        self.assertEqual(positions["300389"]["name"], "艾比森")
        self.assertEqual(positions["300389"]["shares"], 1000)
        self.assertEqual(positions["300389"]["current_price"], 19.5)
        self.assertEqual(positions["603063"]["name"], "禾望电气")
        self.assertEqual(positions["603063"]["shares"], 600)
        self.assertEqual(positions["603063"]["current_price"], 33.1)

        cn_snapshot = load_portfolio_snapshot(scope="cn_a_system")
        self.assertEqual(cn_snapshot["summary"]["holding_count"], 0)
        self.assertEqual(cn_snapshot["summary"]["current_exposure"], 0.0)

        with mock.patch("scripts.state.service.MXMoni", side_effect=RuntimeError("broker unavailable")):
            cached_snapshot = load_portfolio_snapshot(scope="paper_mx")

        self.assertEqual(cached_snapshot["summary"]["holding_count"], 2)
        self.assertEqual(cached_snapshot["summary"]["cash_value"], 125000.0)
        self.assertEqual(cached_snapshot["summary"]["total_capital"], 350000.0)

    def test_sync_portfolio_state_refreshes_paper_mx_contract(self):
        from scripts.state import load_portfolio_snapshot, sync_portfolio_state

        broker = mock.Mock()
        broker.positions.return_value = {
            "code": "200",
            "data": {
                "posList": [
                    {
                        "stockCode": "300389",
                        "stockName": "艾比森",
                        "totalQty": 1000,
                        "costPrice": 19.13,
                        "lastPrice": 19.50,
                        "marketValue": 19500,
                    },
                ]
            },
        }
        broker.balance.return_value = {
            "code": "200",
            "data": {
                "totalAssets": 30000,
                "availBalance": 10500,
                "totalPosValue": 19500,
            },
        }

        with mock.patch("scripts.state.service.MXMoni", return_value=broker):
            result = sync_portfolio_state()

        self.assertEqual(result["paper_mx"]["status"], "success")
        self.assertEqual(result["paper_mx"]["positions"], 1)

        with mock.patch("scripts.state.service.MXMoni", side_effect=RuntimeError("broker unavailable")):
            snapshot = load_portfolio_snapshot(scope="paper_mx")

        self.assertEqual(snapshot["summary"]["holding_count"], 1)
        self.assertEqual(snapshot["summary"]["cash_value"], 10500.0)
        self.assertEqual(snapshot["summary"]["total_capital"], 30000.0)
