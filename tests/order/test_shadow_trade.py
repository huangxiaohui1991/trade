#!/usr/bin/env python3
"""
tests/order/test_shadow_trade.py

测试 scripts/pipeline/shadow_trade.py 的核心函数：
  - check_stop_signals
  - buy_new_picks
  - reconcile_trade_state
  - get_status
"""

import os
import tempfile
import unittest
from datetime import date, datetime
from unittest import mock


class ShadowTradeTests(unittest.TestCase):
    def setUp(self):
        self._old_db_path = os.environ.get("TRADE_STATE_DB_PATH")
        self._old_vault = os.environ.get("AStockVault")
        self._tmpdir = tempfile.TemporaryDirectory()
        os.environ["TRADE_STATE_DB_PATH"] = os.path.join(self._tmpdir.name, "trade_state.sqlite3")
        os.environ["AStockVault"] = self._tmpdir.name

    def tearDown(self):
        if self._old_db_path is None:
            os.environ.pop("TRADE_STATE_DB_PATH", None)
        else:
            os.environ["TRADE_STATE_DB_PATH"] = self._old_db_path
        if self._old_vault is None:
            os.environ.pop("AStockVault", None)
        else:
            os.environ["AStockVault"] = self._old_vault
        self._tmpdir.cleanup()

    def _bootstrap_empty_ledger(self):
        from scripts.state.service import bootstrap_state

        with mock.patch(
            "scripts.state.service._bootstrap_portfolio_snapshot",
            return_value=([], [], "2026-04-09"),
        ), mock.patch(
            "scripts.state.service._bootstrap_trade_events",
            return_value=[],
        ), mock.patch(
            "scripts.state.service._bootstrap_pool_entries",
            return_value=[],
        ):
            bootstrap_state(force=True)

    # -------------------------------------------------------------------------
    # check_stop_signals tests
    # -------------------------------------------------------------------------

    def test_check_stop_signals_dynamic_stop_loss_triggers(self):
        """
        动态止损触发场景：
        持仓现价跌破成本价的 stop_loss_pct（默认4%），check_stop_signals
        应识别出动态止损信号，action 为清仓，reason_code 为 RISK_DYNAMIC_STOP。
        """
        from scripts.pipeline.shadow_trade import check_stop_signals

        self._bootstrap_empty_ledger()

        # 持仓成本 20 元，动态止损 4% → 止损价 19.20
        # 现价 18.80 → 亏损 6% < -4% → 触发动态止损
        mock_positions = [
            {
                "secCode": "300389",
                "secName": "艾比森",
                "count": 1000,
                "costPrice": 20000,   # priceDec=2 → 20000/100=20.0
                "costPriceDec": 2,
                "price": 18800,      # priceDec=2 → 18800/100=18.80
                "priceDec": 2,
                "value": 188000,
                "dayProfit": -12000,
                "dayProfitPct": -6.0,
                "enableQty": 1000,
            }
        ]

        with mock.patch(
            "scripts.pipeline.shadow_trade._get_positions",
            return_value=mock_positions,
        ), mock.patch(
            "scripts.pipeline.shadow_trade._load_paper_position_context",
            return_value={},
        ), mock.patch(
            "scripts.pipeline.shadow_trade._submit_shadow_order",
            return_value=({"code": "200", "orderId": "ORD-STOP1"}, {"status": "filled"}),
        ), mock.patch(
            "scripts.pipeline.shadow_trade.get_strategy",
            return_value={"risk": {"stop_loss": 0.04, "absolute_stop": 0.07, "take_profit": {"t1_pct": 0.15}}},
        ), mock.patch(
            "scripts.pipeline.shadow_trade.get_stocks",
            return_value={"core_pool": []},
        ):
            results = check_stop_signals(dry_run=True)

        self.assertEqual(len(results), 1)
        result = results[0]
        self.assertEqual(result["code"], "300389")
        self.assertEqual(result["action"], "清仓")
        self.assertEqual(result["reason_code"], "RISK_DYNAMIC_STOP")
        self.assertEqual(result["status"], "dry_run")
        self.assertIn("动态止损", result["reason"])

    def test_check_stop_signals_absolute_stop_loss_triggers(self):
        """
        绝对止损触发场景：
        持仓现价跌破成本价的 absolute_stop_pct（默认7%），应优先于动态止损
        触发绝对止损，action 为清仓，reason_code 为 RISK_ABSOLUTE_STOP。
        """
        from scripts.pipeline.shadow_trade import check_stop_signals

        self._bootstrap_empty_ledger()

        # 持仓成本 20 元，绝对止损 7% → 止损价 18.60
        # 现价 18.00 → 亏损 10% < -7% → 触发绝对止损
        mock_positions = [
            {
                "secCode": "603063",
                "secName": "禾望电气",
                "count": 1000,
                "costPrice": 20000,   # 20.00
                "costPriceDec": 2,
                "price": 18000,      # 18.00
                "priceDec": 2,
                "value": 180000,
                "dayProfit": -20000,
                "dayProfitPct": -10.0,
                "enableQty": 1000,
            }
        ]

        with mock.patch(
            "scripts.pipeline.shadow_trade._get_positions",
            return_value=mock_positions,
        ), mock.patch(
            "scripts.pipeline.shadow_trade._load_paper_position_context",
            return_value={},
        ), mock.patch(
            "scripts.pipeline.shadow_trade._submit_shadow_order",
            return_value=({"code": "200", "orderId": "ORD-STOP2"}, {"status": "placed"}),
        ), mock.patch(
            "scripts.pipeline.shadow_trade.get_strategy",
            return_value={"risk": {"stop_loss": 0.04, "absolute_stop": 0.07, "take_profit": {"t1_pct": 0.15}}},
        ), mock.patch(
            "scripts.pipeline.shadow_trade.get_stocks",
            return_value={"core_pool": []},
        ):
            results = check_stop_signals(dry_run=True)

        self.assertEqual(len(results), 1)
        result = results[0]
        self.assertEqual(result["code"], "603063")
        self.assertEqual(result["action"], "清仓")
        self.assertEqual(result["reason_code"], "RISK_ABSOLUTE_STOP")
        self.assertIn("绝对止损", result["reason"])

    def test_check_stop_signals_take_profit_t1_triggers(self):
        """
        第一批止盈触发场景：
        持仓现价超过成本价的 t1_pct（默认15%），应触发部分止盈，
        卖出四分之一持仓，reason_code 为 RISK_TAKE_PROFIT_T1。
        """
        from scripts.pipeline.shadow_trade import check_stop_signals

        self._bootstrap_empty_ledger()

        # 持仓成本 20 元，止盈 15% → 止盈价 23.00
        # 现价 23.50 → 盈利 17.5% > 15% → 触发第一批止盈
        mock_positions = [
            {
                "secCode": "000612",
                "secName": "焦作万方",
                "count": 1000,
                "costPrice": 20000,   # 20.00
                "costPriceDec": 2,
                "price": 23500,      # 23.50
                "priceDec": 2,
                "value": 235000,
                "dayProfit": 35000,
                "dayProfitPct": 17.5,
                "enableQty": 1000,
            }
        ]

        with mock.patch(
            "scripts.pipeline.shadow_trade._get_positions",
            return_value=mock_positions,
        ), mock.patch(
            "scripts.pipeline.shadow_trade._load_paper_position_context",
            return_value={},
        ), mock.patch(
            "scripts.pipeline.shadow_trade._submit_shadow_order",
            return_value=({"code": "200", "orderId": "ORD-TP1"}, {"status": "filled"}),
        ), mock.patch(
            "scripts.pipeline.shadow_trade.get_strategy",
            return_value={"risk": {"stop_loss": 0.04, "absolute_stop": 0.07, "take_profit": {"t1_pct": 0.15}}},
        ), mock.patch(
            "scripts.pipeline.shadow_trade.get_stocks",
            return_value={"core_pool": []},
        ):
            results = check_stop_signals(dry_run=True)

        self.assertEqual(len(results), 1)
        result = results[0]
        self.assertEqual(result["code"], "000612")
        self.assertIn("卖出", result["action"])
        self.assertEqual(result["reason_code"], "RISK_TAKE_PROFIT_T1")
        self.assertIn("止盈第一批", result["reason"])

    def test_check_stop_signals_time_stop_advisory(self):
        """
        时间止损提示场景（advisory，不自动下单）：
        持仓超过 time_stop_days（默认15天）且涨幅低于2%，
        应生成 RISK_TIME_STOP advisory 信号，action 为持有，不自动卖出。
        """
        from scripts.pipeline.shadow_trade import check_stop_signals

        self._bootstrap_empty_ledger()

        mock_positions = [
            {
                "secCode": "300389",
                "secName": "艾比森",
                "count": 1000,
                "costPrice": 20000,
                "costPriceDec": 2,
                "price": 20000,   # 价格未涨，盈亏 ~0%
                "priceDec": 2,
                "value": 200000,
                "dayProfit": 0,
                "dayProfitPct": 0.0,
                "enableQty": 1000,
            }
        ]

        today = date(2026, 4, 12)
        open_date = date(2026, 3, 20)  # 23天前，超过15天
        trade_events = [
            {
                "code": "300389",
                "name": "艾比森",
                "side": "buy",
                "shares": 1000,
                "price": 20.0,
                "event_date": open_date.isoformat(),
                "created_at": "2026-03-20T10:00:00",
            }
        ]

        with mock.patch(
            "scripts.pipeline.shadow_trade._get_positions",
            return_value=mock_positions,
        ), mock.patch(
            "scripts.pipeline.shadow_trade._load_paper_position_context",
            return_value={
                "300389": {
                    "open_date": open_date.isoformat(),
                    "first_buy_price": 20.0,
                    "net_shares": 1000,
                }
            },
        ), mock.patch(
            "scripts.pipeline.shadow_trade.get_strategy",
            return_value={
                "risk": {
                    "stop_loss": 0.04,
                    "absolute_stop": 0.07,
                    "take_profit": {"t1_pct": 0.15, "t1_drawdown": 0.05, "t2_drawdown": 0.08},
                    "time_stop_days": 15,
                }
            },
        ), mock.patch(
            "scripts.pipeline.shadow_trade.get_stocks",
            return_value={"core_pool": []},
        ):
            with mock.patch(
                "scripts.pipeline.shadow_trade._load_history_points_since",
                return_value=[],
            ):
                results = check_stop_signals(dry_run=True)

        self.assertEqual(len(results), 1)
        result = results[0]
        self.assertEqual(result["action"], "持有")
        self.assertEqual(result["reason_code"], "RISK_HOLD")
        advisory_signals = result.get("advisory_signals", [])
        self.assertEqual(len(advisory_signals), 1)
        self.assertEqual(advisory_signals[0]["rule_code"], "RISK_TIME_STOP")
        self.assertEqual(advisory_signals[0]["severity"], "warning")

    def test_check_stop_signals_drawdown_take_profit_advisory(self):
        """
        回撤止盈提示场景（advisory，不自动下单）：
        持仓从高点回撤超过 t1_drawdown（默认5%），
        应生成 RISK_DRAWDOWN_TAKE_PROFIT advisory 信号。
        """
        from scripts.pipeline.shadow_trade import check_stop_signals

        self._bootstrap_empty_ledger()

        mock_positions = [
            {
                "secCode": "300389",
                "secName": "艾比森",
                "count": 1000,
                "costPrice": 20000,
                "costPriceDec": 2,
                "price": 22800,   # 现价 22.80，盈利 14%
                "priceDec": 2,
                "value": 228000,
                "dayProfit": 28000,
                "dayProfitPct": 14.0,
                "enableQty": 1000,
            }
        ]

        today = date(2026, 4, 12)
        open_date = date(2026, 4, 1)

        with mock.patch(
            "scripts.pipeline.shadow_trade._get_positions",
            return_value=mock_positions,
        ), mock.patch(
            "scripts.pipeline.shadow_trade._load_paper_position_context",
            return_value={
                "300389": {
                    "open_date": open_date.isoformat(),
                    "first_buy_price": 20.0,
                    "net_shares": 1000,
                }
            },
        ), mock.patch(
            "scripts.pipeline.shadow_trade.get_strategy",
            return_value={
                "risk": {
                    "stop_loss": 0.04,
                    "absolute_stop": 0.07,
                    "take_profit": {"t1_pct": 0.15, "t1_drawdown": 0.05, "t2_drawdown": 0.08},
                    "time_stop_days": 15,
                }
            },
        ), mock.patch(
            "scripts.pipeline.shadow_trade.get_stocks",
            return_value={"core_pool": []},
        ):
            # 模拟历史高点：24.00（已超过成本*1.15=23.00），回撤到22.80
            # 回撤 = (24.00-22.80)/24.00 = 5%
            history_points = [
                {"date": "2026-04-05", "close": 23000},
                {"date": "2026-04-08", "close": 24000},  # 高点
                {"date": "2026-04-10", "close": 22800},  # 现价
            ]
            with mock.patch(
                "scripts.pipeline.shadow_trade._load_history_points_since",
                return_value=history_points,
            ):
                results = check_stop_signals(dry_run=True)

        self.assertEqual(len(results), 1)
        result = results[0]
        advisory_signals = result.get("advisory_signals", [])
        self.assertTrue(len(advisory_signals) >= 1)
        drawdown_signal = next(
            (s for s in advisory_signals if s["rule_code"] == "RISK_DRAWDOWN_TAKE_PROFIT"),
            None,
        )
        self.assertIsNotNone(drawdown_signal)
        self.assertIn("回撤", drawdown_signal["message"])

    def test_check_stop_signals_no_positions_returns_empty(self):
        """
        空仓场景：_get_positions 返回空列表，check_stop_signals 直接返回空结果。
        """
        from scripts.pipeline.shadow_trade import check_stop_signals

        self._bootstrap_empty_ledger()

        with mock.patch(
            "scripts.pipeline.shadow_trade._get_positions",
            return_value=[],
        ), mock.patch(
            "scripts.pipeline.shadow_trade._load_paper_position_context",
            return_value={},
        ), mock.patch(
            "scripts.pipeline.shadow_trade.get_strategy",
            return_value={"risk": {}},
        ):
            results = check_stop_signals()

        self.assertEqual(results, [])

    def test_check_stop_signals_hold_when_no_signal(self):
        """
        无止损止盈信号时：持仓盈亏在正常范围，应 action=持有，reason_code=RISK_HOLD。
        """
        from scripts.pipeline.shadow_trade import check_stop_signals

        self._bootstrap_empty_ledger()

        # 持仓成本 20 元，现价 21 元，盈利 5%，在 -4%~+15% 之间，无信号
        mock_positions = [
            {
                "secCode": "300389",
                "secName": "艾比森",
                "count": 1000,
                "costPrice": 20000,
                "costPriceDec": 2,
                "price": 21000,
                "priceDec": 2,
                "value": 210000,
                "dayProfit": 10000,
                "dayProfitPct": 5.0,
                "enableQty": 1000,
            }
        ]

        with mock.patch(
            "scripts.pipeline.shadow_trade._get_positions",
            return_value=mock_positions,
        ), mock.patch(
            "scripts.pipeline.shadow_trade._load_paper_position_context",
            return_value={},
        ), mock.patch(
            "scripts.pipeline.shadow_trade.get_strategy",
            return_value={"risk": {"stop_loss": 0.04, "absolute_stop": 0.07, "take_profit": {"t1_pct": 0.15}}},
        ), mock.patch(
            "scripts.pipeline.shadow_trade.get_stocks",
            return_value={"core_pool": []},
        ):
            results = check_stop_signals(dry_run=True)

        self.assertEqual(len(results), 1)
        result = results[0]
        self.assertEqual(result["action"], "持有")
        self.assertEqual(result["reason_code"], "RISK_HOLD")

    # -------------------------------------------------------------------------
    # buy_new_picks tests
    # -------------------------------------------------------------------------

    def test_buy_new_picks_no_holdings_buys_all(self):
        """
        无持仓时买入：核心池有股票、评分达标、无持仓、无 veto → 执行市价买入。
        """
        from scripts.pipeline.shadow_trade import buy_new_picks

        self._bootstrap_empty_ledger()

        mock_stocks = {
            "core_pool": [
                {"code": "300389", "name": "艾比森", "score": 8.1},
            ]
        }

        with mock.patch(
            "scripts.pipeline.shadow_trade.get_stocks",
            return_value=mock_stocks,
        ), mock.patch(
            "scripts.pipeline.shadow_trade._get_positions",
            return_value=[],
        ), mock.patch(
            "scripts.pipeline.shadow_trade._get_balance",
            return_value={"available": 200000, "total_assets": 200000},
        ), mock.patch(
            "scripts.pipeline.shadow_trade._query_mx",
            return_value={
                "data": {
                    "data": {
                        "searchDataResultDTO": {
                            "dataTableDTOList": [
                                {
                                    "table": {
                                        "headName": "最新价",
                                        "1": ["20.50元"],
                                    }
                                }
                            ]
                        }
                    }
                }
            },
        ), mock.patch(
            "scripts.pipeline.shadow_trade.get_strategy",
            return_value={"scoring": {"thresholds": {"buy": 7}}},
        ), mock.patch(
            "scripts.pipeline.shadow_trade.score_stock",
            return_value={
                "total_score": 8.1,
                "data_quality": "ok",
                "data_missing_fields": [],
                "veto_signals": [],
            },
        ), mock.patch(
            "scripts.pipeline.shadow_trade._submit_shadow_order",
            return_value=({"code": "200", "orderId": "ORD-B1"}, {"status": "filled", "external_id": "ext-1"}),
        ), mock.patch(
            "scripts.pipeline.shadow_trade._calc_shares",
            return_value=900,
        ):
            results = buy_new_picks(dry_run=False)

        self.assertEqual(len(results), 1)
        result = results[0]
        self.assertEqual(result["code"], "300389")
        self.assertEqual(result["name"], "艾比森")
        self.assertEqual(result["status"], "成功")
        self.assertEqual(result["shares"], 900)

    def test_buy_new_picks_already_held_skipped(self):
        """
        有持仓时跳过：股票已在模拟盘持仓中，buy_new_picks 应跳过不买入，
        结果 status=跳过，reason_code=POSITION_HELD。
        """
        from scripts.pipeline.shadow_trade import buy_new_picks

        self._bootstrap_empty_ledger()

        mock_stocks = {
            "core_pool": [
                {"code": "300389", "name": "艾比森"},
            ]
        }
        mock_positions = [
            {"stockCode": "300389", "secuCode": "", "count": 1000},
        ]

        with mock.patch(
            "scripts.pipeline.shadow_trade.get_stocks",
            return_value=mock_stocks,
        ), mock.patch(
            "scripts.pipeline.shadow_trade._get_positions",
            return_value=mock_positions,
        ), mock.patch(
            "scripts.pipeline.shadow_trade._get_balance",
            return_value={"available": 200000},
        ), mock.patch(
            "scripts.pipeline.shadow_trade.get_strategy",
            return_value={"scoring": {"thresholds": {"buy": 7}}},
        ), mock.patch(
            "scripts.pipeline.shadow_trade.score_stock",
            return_value={
                "total_score": 8.1,
                "data_quality": "ok",
                "data_missing_fields": [],
                "veto_signals": [],
            },
        ):
            results = buy_new_picks()

        self.assertEqual(len(results), 1)
        result = results[0]
        self.assertEqual(result["status"], "跳过")
        self.assertEqual(result["reason_code"], "POSITION_HELD")

    def test_buy_new_picks_no_new_picks_returns_empty(self):
        """
        核心池为空时：buy_new_picks 应直接返回空列表，不做任何操作。
        """
        from scripts.pipeline.shadow_trade import buy_new_picks

        self._bootstrap_empty_ledger()

        with mock.patch(
            "scripts.pipeline.shadow_trade.get_stocks",
            return_value={"core_pool": []},
        ), mock.patch(
            "scripts.pipeline.shadow_trade._get_positions",
            return_value=[],
        ):
            results = buy_new_picks()

        self.assertEqual(results, [])

    def test_buy_new_picks_score_too_low_skipped(self):
        """
        评分不足时跳过：股票评分低于阈值，结果 status=跳过，reason_code=SCORE_TOO_LOW。
        """
        from scripts.pipeline.shadow_trade import buy_new_picks

        self._bootstrap_empty_ledger()

        mock_stocks = {
            "core_pool": [
                {"code": "300389", "name": "艾比森"},
            ]
        }

        with mock.patch(
            "scripts.pipeline.shadow_trade.get_stocks",
            return_value=mock_stocks,
        ), mock.patch(
            "scripts.pipeline.shadow_trade._get_positions",
            return_value=[],
        ), mock.patch(
            "scripts.pipeline.shadow_trade._get_balance",
            return_value={"available": 200000},
        ), mock.patch(
            "scripts.pipeline.shadow_trade.get_strategy",
            return_value={"scoring": {"thresholds": {"buy": 7}}},
        ), mock.patch(
            "scripts.pipeline.shadow_trade.score_stock",
            return_value={
                "total_score": 5.5,
                "data_quality": "ok",
                "data_missing_fields": [],
                "veto_signals": [],
            },
        ):
            results = buy_new_picks()

        self.assertEqual(len(results), 1)
        result = results[0]
        self.assertEqual(result["status"], "跳过")
        self.assertEqual(result["reason_code"], "SCORE_TOO_LOW")

    def test_buy_new_picks_veto_signal_skipped(self):
        """
        veto 信号跳过：股票触发一票否决，result status=跳过，reason_code=POOL_VETO。
        """
        from scripts.pipeline.shadow_trade import buy_new_picks

        self._bootstrap_empty_ledger()

        mock_stocks = {
            "core_pool": [
                {"code": "300389", "name": "艾比森"},
            ]
        }

        with mock.patch(
            "scripts.pipeline.shadow_trade.get_stocks",
            return_value=mock_stocks,
        ), mock.patch(
            "scripts.pipeline.shadow_trade._get_positions",
            return_value=[],
        ), mock.patch(
            "scripts.pipeline.shadow_trade._get_balance",
            return_value={"available": 200000},
        ), mock.patch(
            "scripts.pipeline.shadow_trade.get_strategy",
            return_value={"scoring": {"thresholds": {"buy": 7}}},
        ), mock.patch(
            "scripts.pipeline.shadow_trade.score_stock",
            return_value={
                "total_score": 8.5,
                "data_quality": "ok",
                "data_missing_fields": [],
                "veto_signals": ["连续跌停"],
            },
        ):
            results = buy_new_picks()

        self.assertEqual(len(results), 1)
        result = results[0]
        self.assertEqual(result["status"], "跳过")
        self.assertEqual(result["reason_code"], "POOL_VETO")

    def test_buy_new_picks_dry_run_no_order_submitted(self):
        """
        dry_run 模式：不调用 _submit_shadow_order，只记录模拟买入信息。
        """
        from scripts.pipeline.shadow_trade import buy_new_picks

        self._bootstrap_empty_ledger()

        mock_stocks = {
            "core_pool": [
                {"code": "300389", "name": "艾比森"},
            ]
        }

        with mock.patch(
            "scripts.pipeline.shadow_trade.get_stocks",
            return_value=mock_stocks,
        ), mock.patch(
            "scripts.pipeline.shadow_trade._get_positions",
            return_value=[],
        ), mock.patch(
            "scripts.pipeline.shadow_trade._get_balance",
            return_value={"available": 200000},
        ), mock.patch(
            "scripts.pipeline.shadow_trade._query_mx",
            return_value={
                "data": {
                    "data": {
                        "searchDataResultDTO": {
                            "dataTableDTOList": [
                                {
                                    "table": {
                                        "headName": "最新价",
                                        "1": ["20.50元"],
                                    }
                                }
                            ]
                        }
                    }
                }
            },
        ), mock.patch(
            "scripts.pipeline.shadow_trade.get_strategy",
            return_value={"scoring": {"thresholds": {"buy": 7}}},
        ), mock.patch(
            "scripts.pipeline.shadow_trade.score_stock",
            return_value={
                "total_score": 8.1,
                "data_quality": "ok",
                "data_missing_fields": [],
                "veto_signals": [],
            },
        ), mock.patch(
            "scripts.pipeline.shadow_trade._submit_shadow_order",
        ) as mock_submit:
            results = buy_new_picks(dry_run=True)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["status"], "dry_run")
        mock_submit.assert_not_called()

    # -------------------------------------------------------------------------
    # reconcile_trade_state tests
    # -------------------------------------------------------------------------

    def test_reconcile_trade_state_consistent_returns_ok(self):
        """
        对账一致场景：事件流与 broker 持仓完全一致，
        reconcile_trade_state 应返回 status=ok，planned_actions 为空。
        """
        from scripts.pipeline.shadow_trade import reconcile_trade_state

        self._bootstrap_empty_ledger()

        # 注入一致的持仓事件：买入 300389 1000 股
        from scripts.state import record_trade_event
        record_trade_event({
            "external_id": "test:reconcile:1",
            "scope": "paper_mx",
            "market": "MX_PAPER",
            "code": "300389",
            "name": "艾比森",
            "side": "buy",
            "event_type": "buy",
            "shares": 1000,
            "price": 20.0,
            "amount": 20000,
            "event_date": "2026-04-09",
            "reason_code": "BUY_CORE_POOL",
            "reason_text": "核心池评分8.1",
            "source": "test",
            "metadata": {},
        })

        with mock.patch(
            "scripts.pipeline.shadow_trade._get_positions",
            return_value=[
                {
                    "secCode": "300389",
                    "secName": "艾比森",
                    "count": 1000,
                    "costPrice": 20000,
                    "costPriceDec": 2,
                    "price": 21000,
                    "priceDec": 2,
                }
            ],
        ), mock.patch(
            "scripts.pipeline.shadow_trade._get_balance",
            return_value={"available": 180000, "total_assets": 200000},
        ), mock.patch(
            "scripts.pipeline.shadow_trade._sync_broker_orders",
            return_value={"status": "ok", "fetched_count": 0, "synced_count": 0},
        ), mock.patch(
            "scripts.pipeline.shadow_trade.get_strategy",
            return_value={"risk": {}},
        ):
            result = reconcile_trade_state(apply=False, window=30)

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["planned_action_count"], 0)
        self.assertEqual(result["planned_actions"], [])

    def test_reconcile_trade_state_event_only_generates_flatten_action(self):
        """
        事件流残留场景（broker_only=False, event_only=True）：
        事件流有持仓但 broker 空仓，应生成 flatten_missing_broker_position 卖出动作。
        """
        from scripts.pipeline.shadow_trade import reconcile_trade_state

        self._bootstrap_empty_ledger()

        # 注入买入事件，broker 侧为空
        from scripts.state import record_trade_event
        record_trade_event({
            "external_id": "test:reconcile:2",
            "scope": "paper_mx",
            "market": "MX_PAPER",
            "code": "300389",
            "name": "艾比森",
            "side": "buy",
            "event_type": "buy",
            "shares": 1000,
            "price": 20.0,
            "amount": 20000,
            "event_date": "2026-04-09",
            "reason_code": "BUY_CORE_POOL",
            "reason_text": "核心池评分8.1",
            "source": "test",
            "metadata": {},
        })

        with mock.patch(
            "scripts.pipeline.shadow_trade._get_positions",
            return_value=[],
        ), mock.patch(
            "scripts.pipeline.shadow_trade._get_balance",
            return_value={"available": 200000},
        ), mock.patch(
            "scripts.pipeline.shadow_trade._sync_broker_orders",
            return_value={"status": "ok"},
        ), mock.patch(
            "scripts.pipeline.shadow_trade.get_strategy",
            return_value={"risk": {}},
        ):
            result = reconcile_trade_state(apply=False, window=30)

        self.assertEqual(result["status"], "drift")
        self.assertEqual(result["planned_action_count"], 1)
        action = result["planned_actions"][0]
        self.assertEqual(action["action"], "flatten_missing_broker_position")
        self.assertEqual(action["side"], "sell")
        self.assertEqual(action["code"], "300389")
        self.assertEqual(action["shares"], 1000)

    def test_reconcile_trade_state_broker_only_generates_open_action(self):
        """
        Broker 独有持仓场景（broker_only=True, event_only=False）：
        broker 持有股票但事件流缺失，应生成 open_missing_event_position 买入动作。
        """
        from scripts.pipeline.shadow_trade import reconcile_trade_state

        self._bootstrap_empty_ledger()

        with mock.patch(
            "scripts.pipeline.shadow_trade._get_positions",
            return_value=[
                {
                    "secCode": "300389",
                    "secName": "艾比森",
                    "count": 1000,
                    "costPrice": 20000,
                    "costPriceDec": 2,
                    "price": 21000,
                    "priceDec": 2,
                }
            ],
        ), mock.patch(
            "scripts.pipeline.shadow_trade._get_balance",
            return_value={"available": 180000},
        ), mock.patch(
            "scripts.pipeline.shadow_trade._sync_broker_orders",
            return_value={"status": "ok"},
        ), mock.patch(
            "scripts.pipeline.shadow_trade.get_strategy",
            return_value={"risk": {}},
        ):
            result = reconcile_trade_state(apply=False, window=30)

        self.assertEqual(result["status"], "drift")
        self.assertEqual(result["planned_action_count"], 1)
        action = result["planned_actions"][0]
        self.assertEqual(action["action"], "open_missing_event_position")
        self.assertEqual(action["side"], "buy")
        self.assertEqual(action["code"], "300389")

    def test_reconcile_trade_state_share_mismatch_broker_excess(self):
        """
        持仓数量不一致场景（broker > event）：
        broker 持有 1500 股，事件流只有 1000 股，
        应生成 increase_event_position_to_broker 买入动作，补入 delta=500 股。
        """
        from scripts.pipeline.shadow_trade import reconcile_trade_state

        self._bootstrap_empty_ledger()

        from scripts.state import record_trade_event
        record_trade_event({
            "external_id": "test:reconcile:3",
            "scope": "paper_mx",
            "market": "MX_PAPER",
            "code": "300389",
            "name": "艾比森",
            "side": "buy",
            "event_type": "buy",
            "shares": 1000,
            "price": 20.0,
            "amount": 20000,
            "event_date": "2026-04-09",
            "reason_code": "BUY_CORE_POOL",
            "reason_text": "核心池",
            "source": "test",
            "metadata": {},
        })

        with mock.patch(
            "scripts.pipeline.shadow_trade._get_positions",
            return_value=[
                {
                    "secCode": "300389",
                    "secName": "艾比森",
                    "count": 1500,     # broker 有 1500 股
                    "costPrice": 20000,
                    "costPriceDec": 2,
                    "price": 21000,
                    "priceDec": 2,
                }
            ],
        ), mock.patch(
            "scripts.pipeline.shadow_trade._get_balance",
            return_value={"available": 170000},
        ), mock.patch(
            "scripts.pipeline.shadow_trade._sync_broker_orders",
            return_value={"status": "ok"},
        ), mock.patch(
            "scripts.pipeline.shadow_trade.get_strategy",
            return_value={"risk": {}},
        ):
            result = reconcile_trade_state(apply=False, window=30)

        self.assertEqual(result["status"], "drift")
        self.assertEqual(result["planned_action_count"], 1)
        action = result["planned_actions"][0]
        self.assertEqual(action["action"], "increase_event_position_to_broker")
        self.assertEqual(action["side"], "buy")
        self.assertEqual(action["code"], "300389")
        self.assertEqual(action["shares"], 500)  # delta = 1500 - 1000

    def test_reconcile_trade_state_share_mismatch_broker_less(self):
        """
        持仓数量不一致场景（broker < event）：
        broker 持有 500 股，事件流有 1000 股，
        应生成 decrease_event_position_to_broker 卖出动作，卖出 delta=500 股。
        """
        from scripts.pipeline.shadow_trade import reconcile_trade_state

        self._bootstrap_empty_ledger()

        from scripts.state import record_trade_event
        record_trade_event({
            "external_id": "test:reconcile:4",
            "scope": "paper_mx",
            "market": "MX_PAPER",
            "code": "300389",
            "name": "艾比森",
            "side": "buy",
            "event_type": "buy",
            "shares": 1000,
            "price": 20.0,
            "amount": 20000,
            "event_date": "2026-04-09",
            "reason_code": "BUY_CORE_POOL",
            "reason_text": "核心池",
            "source": "test",
            "metadata": {},
        })

        with mock.patch(
            "scripts.pipeline.shadow_trade._get_positions",
            return_value=[
                {
                    "secCode": "300389",
                    "secName": "艾比森",
                    "count": 500,      # broker 只有 500 股
                    "costPrice": 20000,
                    "costPriceDec": 2,
                    "price": 21000,
                    "priceDec": 2,
                }
            ],
        ), mock.patch(
            "scripts.pipeline.shadow_trade._get_balance",
            return_value={"available": 190000},
        ), mock.patch(
            "scripts.pipeline.shadow_trade._sync_broker_orders",
            return_value={"status": "ok"},
        ), mock.patch(
            "scripts.pipeline.shadow_trade.get_strategy",
            return_value={"risk": {}},
        ):
            result = reconcile_trade_state(apply=False, window=30)

        self.assertEqual(result["status"], "drift")
        self.assertEqual(result["planned_action_count"], 1)
        action = result["planned_actions"][0]
        self.assertEqual(action["action"], "decrease_event_position_to_broker")
        self.assertEqual(action["side"], "sell")
        self.assertEqual(action["code"], "300389")
        self.assertEqual(action["shares"], 500)  # delta = abs(500 - 1000)

    def test_reconcile_trade_state_apply_writes_log(self):
        """
        apply=True 时：reconcile_trade_state 应调用 _log_trade 记录每条补偿动作。
        """
        from scripts.pipeline.shadow_trade import reconcile_trade_state

        self._bootstrap_empty_ledger()

        # 注入买入事件，broker 侧为空
        from scripts.state import record_trade_event
        record_trade_event({
            "external_id": "test:reconcile:5",
            "scope": "paper_mx",
            "market": "MX_PAPER",
            "code": "300389",
            "name": "艾比森",
            "side": "buy",
            "event_type": "buy",
            "shares": 1000,
            "price": 20.0,
            "amount": 20000,
            "event_date": "2026-04-09",
            "reason_code": "BUY_CORE_POOL",
            "reason_text": "核心池",
            "source": "test",
            "metadata": {},
        })

        with mock.patch(
            "scripts.pipeline.shadow_trade._get_positions",
            return_value=[],
        ), mock.patch(
            "scripts.pipeline.shadow_trade._get_balance",
            return_value={"available": 200000},
        ), mock.patch(
            "scripts.pipeline.shadow_trade._sync_broker_orders",
            return_value={"status": "ok"},
        ), mock.patch(
            "scripts.pipeline.shadow_trade.get_strategy",
            return_value={"risk": {}},
        ), mock.patch(
            "scripts.pipeline.shadow_trade._log_trade",
        ) as mock_log:
            result = reconcile_trade_state(apply=True, window=30)

        self.assertEqual(result["planned_action_count"], 1)
        self.assertEqual(len(result["applied_actions"]), 1)
        mock_log.assert_called_once()
        call_args = mock_log.call_args
        self.assertEqual(call_args[0][0], "卖出")  # action text
        self.assertEqual(call_args[0][1], "300389")  # code

    # -------------------------------------------------------------------------
    # get_status tests
    # -------------------------------------------------------------------------

    def test_get_status_returns_expected_structure(self):
        """
        get_status 返回完整状态结构：balance, positions, orders, mx_health 等字段均存在。
        """
        from scripts.pipeline.shadow_trade import get_status

        self._bootstrap_empty_ledger()

        mock_positions = [
            {
                "secCode": "300389",
                "secName": "艾比森",
                "count": 1000,
                "costPrice": 20000,
                "costPriceDec": 3,
                "price": 21000,
                "priceDec": 3,
                "value": 210000,
                "dayProfit": 10000,
                "dayProfitPct": 5.0,
                "enableQty": 1000,
            }
        ]

        with mock.patch(
            "scripts.pipeline.shadow_trade._get_balance",
            return_value={
                "total_assets": 210000,
                "available": 180000,
                "position_value": 210000,
                "total_profit": 10000,
                "init_money": 200000,
            },
        ), mock.patch(
            "scripts.pipeline.shadow_trade._get_positions",
            return_value=mock_positions,
        ), mock.patch(
            "scripts.pipeline.shadow_trade._sync_broker_orders",
            return_value={"status": "ok", "fetched_count": 0, "synced_count": 0},
        ), mock.patch(
            "scripts.pipeline.shadow_trade.get_strategy",
            return_value={
                "risk": {
                    "stop_loss": 0.04,
                    "absolute_stop": 0.07,
                    "take_profit": {"t1_pct": 0.15, "t1_drawdown": 0.05, "t2_drawdown": 0.08},
                    "time_stop_days": 15,
                }
            },
        ), mock.patch(
            "scripts.pipeline.shadow_trade._load_paper_position_context",
            return_value={},
        ), mock.patch(
            "scripts.pipeline.shadow_trade._mx_health_snapshot",
            return_value={
                "status": "ok",
                "available_count": 10,
                "unavailable_count": 0,
                "command_count": 10,
                "groups": {"mx": 10},
                "required": {},
                "unavailable_commands": [],
                "source": "test",
            },
        ):
            status = get_status()

        # 检查顶层字段
        self.assertIn("balance", status)
        self.assertIn("positions", status)
        self.assertIn("orders", status)
        self.assertIn("mx_health", status)
        self.assertIn("advisory_summary", status)
        self.assertIn("timestamp", status)
        self.assertIn("automation_scope", status)
        self.assertIn("automated_rules", status)
        self.assertIn("advisory_rules", status)

        # 检查 balance 内容
        self.assertEqual(status["balance"]["total_assets"], 210000)
        self.assertEqual(status["balance"]["available"], 180000)
        self.assertEqual(status["balance"]["init_money"], 200000)

        # 检查持仓
        self.assertEqual(len(status["positions"]), 1)
        self.assertEqual(status["positions"][0]["code"], "300389")
        self.assertEqual(status["positions"][0]["shares"], 1000)
        self.assertAlmostEqual(status["positions"][0]["cost"], 20.0, places=2)
        self.assertAlmostEqual(status["positions"][0]["price"], 21.0, places=2)

        # 检查 automated/advisory rules
        self.assertIn("RISK_DYNAMIC_STOP", status["automated_rules"])
        self.assertIn("RISK_ABSOLUTE_STOP", status["automated_rules"])
        self.assertIn("RISK_TIME_STOP", status["advisory_rules"])
        self.assertIn("RISK_DRAWDOWN_TAKE_PROFIT", status["advisory_rules"])


if __name__ == "__main__":
    unittest.main()
