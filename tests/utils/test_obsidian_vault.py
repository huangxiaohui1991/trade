import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.utils.config_loader import clear_config_cache
from scripts.utils.obsidian import ObsidianVault, default_vault_path


class DefaultVaultPathTests(unittest.TestCase):
    def setUp(self):
        clear_config_cache()
        self._old_vault = os.environ.get("AStockVault")

    def tearDown(self):
        clear_config_cache()
        if self._old_vault is None:
            os.environ.pop("AStockVault", None)
        else:
            os.environ["AStockVault"] = self._old_vault

    def test_env_path_takes_precedence(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            os.environ["AStockVault"] = tmpdir
            resolved = default_vault_path(Path("/tmp/project"))
            self.assertEqual(resolved, str(Path(tmpdir).resolve()))

    def test_relative_config_path_resolves_from_project_root(self):
        os.environ.pop("AStockVault", None)
        project_root = Path("/tmp/demo-repo")
        with mock.patch("scripts.utils.obsidian.get_paths", return_value={"vault_path": "trade-vault"}):
            resolved = default_vault_path(project_root)
        self.assertEqual(resolved, str((project_root / "trade-vault").resolve()))

    def test_write_account_overview_and_today_decision(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            vault = ObsidianVault(tmpdir)
            account_path = vault.write_account_overview(
                {
                    "summary": {
                        "holding_count": 2,
                        "current_exposure": 0.35,
                        "cash_value": 120000,
                        "total_capital": 300000,
                    },
                    "balances": [
                        {
                            "scope": "cn_a_system",
                            "cash_value": 120000,
                            "total_capital": 300000,
                            "total_market_value": 180000,
                            "exposure": 0.6,
                            "source": "bootstrap:portfolio",
                            "metadata": {
                                "account_overview": [
                                    {"项目": "可交易现金", "金额": "¥120,000", "说明": "测试现金"},
                                ]
                            },
                        }
                    ],
                    "positions": [
                        {
                            "scope": "cn_a_system",
                            "name": "艾比森",
                            "code": "300389",
                            "shares": 1000,
                            "avg_cost": 19.13,
                            "current_price": 19.50,
                            "market_value": 19500,
                        }
                    ],
                },
                {
                    "summary": {
                        "holding_count": 1,
                        "current_exposure": 0.2,
                        "cash_value": 80000,
                        "total_capital": 100000,
                    },
                    "balances": [
                        {
                            "scope": "paper_mx",
                            "cash_value": 80000,
                            "total_capital": 100000,
                            "total_market_value": 20000,
                            "exposure": 0.2,
                            "source": "broker:mx_moni",
                            "metadata": {
                                "balance": {"success": True, "message": ""},
                                "positions": {"success": False, "message": "请求频率过高，请稍后再试", "code": 112},
                            },
                        }
                    ],
                    "positions": [
                        {
                            "scope": "paper_mx",
                            "name": "沪电股份",
                            "code": "002463",
                            "shares": 500,
                            "avg_cost": 32.1,
                            "current_price": 33.0,
                            "market_value": 16500,
                            "status": "持仓",
                        }
                    ],
                },
            )
            decision_path = vault.write_today_decision(
                {
                    "action": "BUY_ALLOWED",
                    "market_signal": "GREEN",
                    "market_multiplier": 1.0,
                    "current_exposure": 0.35,
                    "weekly_buys": 1,
                    "holding_count": 2,
                    "risk": {"can_buy": True},
                    "portfolio_risk": {"state": "ok"},
                    "reasons": ["market_signal=GREEN"],
                    "reason_codes": ["MARKET_GREEN"],
                }
            )

            account_text = Path(account_path).read_text(encoding="utf-8")
            decision_text = Path(decision_path).read_text(encoding="utf-8")

            self.assertIn("账户总览", account_text)
            self.assertIn("快照总览", account_text)
            self.assertIn("数据提示", account_text)
            self.assertIn("实盘补充摘录", account_text)
            self.assertIn("艾比森", account_text)
            self.assertIn("沪电股份", account_text)
            self.assertIn("portfolio.md", account_text)
            self.assertIn("MX 模拟盘", account_text)
            self.assertIn("请求频率过高，请稍后再试", account_text)
            self.assertIn("今日决策", decision_text)
            self.assertIn("BUY_ALLOWED", decision_text)
            self.assertIn("MARKET_GREEN", decision_text)
