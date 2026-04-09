from datetime import date, timedelta
import unittest
from unittest import mock


class PortfolioRiskTests(unittest.TestCase):
    def test_check_portfolio_risk_blocks_daily_loss_limit(self):
        from scripts.engine.risk_model import check_portfolio_risk

        result = check_portfolio_risk(
            trade_events=[
                {
                    "event_date": "2026-04-09",
                    "side": "sell",
                    "realized_pnl": -15000,
                }
            ],
            positions=[],
            total_capital=300000,
            today=date(2026, 4, 9),
            strategy={"risk": {"portfolio": {"daily_loss_limit_pct": 0.03}}},
        )

        self.assertFalse(result["can_trade"])
        self.assertEqual(result["state"], "block")
        self.assertIn("TRADE_PORTFOLIO_DAILY_LOSS_LIMIT", result["reason_codes"])
        self.assertAlmostEqual(result["metrics"]["today_loss_pct"], 0.05, places=4)

    def test_check_portfolio_risk_blocks_consecutive_loss_cooldown(self):
        from scripts.engine.risk_model import check_portfolio_risk

        result = check_portfolio_risk(
            trade_events=[
                {"event_date": "2026-04-08", "side": "sell", "realized_pnl": -500},
                {"event_date": "2026-04-09", "side": "sell", "realized_pnl": -400},
            ],
            positions=[],
            total_capital=100000,
            today=date(2026, 4, 9),
            strategy={
                "risk": {
                    "portfolio": {
                        "daily_loss_limit_pct": 0.03,
                        "consecutive_loss_days_limit": 2,
                        "cooldown_days": 2,
                    }
                }
            },
        )

        self.assertFalse(result["can_trade"])
        self.assertEqual(result["state"], "block")
        self.assertIn("TRADE_CONSECUTIVE_LOSS_COOLDOWN", result["reason_codes"])
        self.assertTrue(result["metrics"]["cooldown_active"])
        self.assertEqual(result["metrics"]["cooldown_until"], "2026-04-11")

    def test_check_portfolio_risk_warns_on_concentration(self):
        from scripts.engine.risk_model import check_portfolio_risk

        result = check_portfolio_risk(
            trade_events=[],
            positions=[{"code": "300389", "market_value": 100000}],
            total_capital=300000,
            today=date(2026, 4, 9),
            strategy={"risk": {"portfolio": {"max_single_position_warn_pct": 0.25}}},
        )

        self.assertTrue(result["can_trade"])
        self.assertEqual(result["state"], "warning")
        self.assertIn("TRADE_POSITION_CONCENTRATION_WARNING", result["reason_codes"])
        self.assertAlmostEqual(result["metrics"]["largest_position_pct"], 0.3333, places=4)

    def test_build_today_decision_blocks_on_portfolio_risk(self):
        today = date.today()
        yesterday = today - timedelta(days=1)
        from scripts.engine.composite import build_today_decision

        with mock.patch("scripts.engine.composite.load_market_snapshot", return_value={"signal": "GREEN"}), mock.patch(
            "scripts.engine.composite.load_portfolio_snapshot",
            return_value={
                "summary": {
                    "current_exposure": 0.1,
                    "holding_count": 1,
                    "total_capital": 100000,
                },
                "positions": [{"code": "300389", "market_value": 30000}],
            },
        ), mock.patch(
            "scripts.engine.composite.load_activity_summary",
            return_value={
                "weekly_buy_count": 0,
                "trade_events": [
                    {"event_date": yesterday.isoformat(), "side": "sell", "realized_pnl": -500},
                    {"event_date": today.isoformat(), "side": "sell", "realized_pnl": -400},
                ],
            },
        ), mock.patch(
            "scripts.engine.composite.check_risk",
            return_value={"can_buy": True, "reasons": [], "limits": {}},
        ):
            result = build_today_decision(
                strategy={
                    "capital": 100000,
                    "risk": {
                        "portfolio": {
                            "daily_loss_limit_pct": 0.03,
                            "consecutive_loss_days_limit": 2,
                            "cooldown_days": 2,
                        }
                    },
                }
            )

        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertEqual(result["action"], "NO_TRADE")
        self.assertIn("TRADE_CONSECUTIVE_LOSS_COOLDOWN", result["reason_codes"])
        self.assertIn("连续亏损冷却中", " ".join(result["reasons"]))
        self.assertEqual(result["portfolio_risk"]["state"], "block")

    def test_build_today_decision_blocks_when_market_is_clear(self):
        from scripts.engine.composite import build_today_decision

        with mock.patch("scripts.engine.composite.load_market_snapshot", return_value={"signal": "CLEAR"}), mock.patch(
            "scripts.engine.composite.load_portfolio_snapshot",
            return_value={
                "summary": {
                    "current_exposure": 0.0,
                    "holding_count": 0,
                    "total_capital": 100000,
                },
                "positions": [],
            },
        ), mock.patch(
            "scripts.engine.composite.load_activity_summary",
            return_value={"weekly_buy_count": 0, "trade_events": []},
        ), mock.patch(
            "scripts.engine.composite.check_risk",
            return_value={"can_buy": True, "reasons": [], "limits": {}},
        ):
            result = build_today_decision(strategy={"capital": 100000})

        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertEqual(result["action"], "NO_TRADE")
        self.assertEqual(result["reasons"], ["market_signal=CLEAR"])


if __name__ == "__main__":
    unittest.main()
