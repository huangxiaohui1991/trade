"""
execution/service.py — 执行服务层

编排订单 + 持仓 + 投影，对外统一接口。
"""

from __future__ import annotations

import sqlite3
from typing import Optional, Protocol, runtime_checkable

from hermes.execution.models import Balance, Order, OrderSide, Position
from hermes.execution.orders import OrderManager
from hermes.execution.positions import PositionManager, PositionProjector
from hermes.platform.events import EventStore


@runtime_checkable
class BrokerAdapter(Protocol):
    """Broker 统一接口。"""

    def submit_order(
        self, code: str, side: str, shares: int, price_cents: int
    ) -> dict:
        """提交订单，返回 {"success": bool, "fill_price_cents": int, "fee_cents": int}。"""
        ...


class SimulatedBroker:
    """回测用 broker — 立即成交，无手续费。"""

    def submit_order(
        self, code: str, side: str, shares: int, price_cents: int
    ) -> dict:
        return {
            "success": True,
            "fill_price_cents": price_cents,
            "fee_cents": 0,
        }


class MXBroker:
    """妙想模拟盘 broker — 通过 MX API 下单。"""

    def submit_order(
        self, code: str, side: str, shares: int, price_cents: int
    ) -> dict:
        try:
            from hermes.market.mx.moni import dispatch_trade_command
            command = f"mx.moni.{side}"
            result = dispatch_trade_command(
                command,
                stock_code=code,
                quantity=shares,
                use_market_price=True,
            )
            trade_code = str(result.get("code", ""))
            if trade_code == "200":
                return {
                    "success": True,
                    "fill_price_cents": price_cents,  # MX 市价单，用请求价近似
                    "fee_cents": 0,
                    "broker_order_id": str(result.get("data", {}).get("orderId", "")),
                }
            else:
                return {
                    "success": False,
                    "error": result.get("message", f"MX API code={trade_code}"),
                }
        except Exception as e:
            return {"success": False, "error": str(e)}


class ExecutionService:
    """执行服务 — 统一入口。"""

    def __init__(
        self,
        event_store: EventStore,
        conn: sqlite3.Connection,
        broker: Optional[BrokerAdapter] = None,
        on_trade: Optional[list] = None,
    ):
        self._events = event_store
        self._conn = conn
        self._orders = OrderManager(event_store, conn)
        self._positions = PositionManager(event_store, conn)
        self._projector = PositionProjector(event_store, conn)
        self._broker = broker or SimulatedBroker()
        self._on_trade = on_trade or []  # list of callable(trade_info: dict)

    # ------------------------------------------------------------------
    # 读操作（从投影表）
    # ------------------------------------------------------------------

    def get_positions(self) -> list[Position]:
        return self._positions.get_positions()

    def get_position(self, code: str) -> Optional[Position]:
        return self._positions.get_position(code)

    def get_portfolio(self) -> dict:
        """从投影表读取组合概览。"""
        positions = self.get_positions()
        total_cost = sum(p.avg_cost_cents * p.shares for p in positions)
        total_market = sum(p.current_price_cents * p.shares for p in positions)

        return {
            "holding_count": len(positions),
            "total_cost_cents": total_cost,
            "total_market_cents": total_market,
            "unrealized_pnl_cents": total_market - total_cost,
            "positions": [p.to_dict() for p in positions],
        }

    def get_order(self, order_id: str) -> Optional[Order]:
        return self._orders.get_order(order_id)

    # ------------------------------------------------------------------
    # 写操作（事件化）
    # ------------------------------------------------------------------

    def execute_buy(
        self,
        code: str,
        name: str,
        shares: int,
        price_cents: int,
        style: str,
        run_id: str,
        broker: str = "",
    ) -> Order:
        """
        买入流程：创建订单 → 提交 broker → 成交 → 开仓。
        """
        order = self._orders.create_order(
            code=code, name=name, side=OrderSide.BUY,
            shares=shares, price_cents=price_cents,
            run_id=run_id, broker=broker,
        )

        result = self._broker.submit_order(code, "buy", shares, price_cents)

        if result["success"]:
            self._orders.fill_order(
                order.order_id,
                fill_price_cents=result["fill_price_cents"],
                fee_cents=result["fee_cents"],
                run_id=run_id,
            )
            self._positions.open_position(
                code=code, name=name, shares=shares,
                avg_cost_cents=result["fill_price_cents"],
                style=style, run_id=run_id,
            )
            self._notify_trade({
                "side": "buy", "code": code, "name": name,
                "shares": shares, "price_cents": result["fill_price_cents"],
                "style": style, "run_id": run_id, "order_id": order.order_id,
            })
        else:
            self._orders.cancel_order(order.order_id, "broker_rejected", run_id)

        return order

    def execute_sell(
        self,
        code: str,
        shares: int,
        price_cents: int,
        run_id: str,
        reason: str = "",
        broker: str = "",
    ) -> Order:
        """
        卖出流程：创建订单 → 提交 broker → 成交 → 清仓。
        """
        pos = self._positions.get_position(code)
        name = pos.name if pos else code

        order = self._orders.create_order(
            code=code, name=name, side=OrderSide.SELL,
            shares=shares, price_cents=price_cents,
            run_id=run_id, broker=broker,
        )

        result = self._broker.submit_order(code, "sell", shares, price_cents)

        if result["success"]:
            self._orders.fill_order(
                order.order_id,
                fill_price_cents=result["fill_price_cents"],
                fee_cents=result["fee_cents"],
                run_id=run_id,
            )
            if pos:
                self._positions.close_position(
                    code=code, shares=shares,
                    sell_price_cents=result["fill_price_cents"],
                    run_id=run_id, reason=reason,
                )
            self._notify_trade({
                "side": "sell", "code": code, "name": name,
                "shares": shares, "price_cents": result["fill_price_cents"],
                "reason": reason, "run_id": run_id, "order_id": order.order_id,
            })
        else:
            self._orders.cancel_order(order.order_id, "broker_rejected", run_id)

        return order

    # ------------------------------------------------------------------
    # 重建
    # ------------------------------------------------------------------

    def rebuild_projections(self) -> list[Position]:
        """从 event_log 完全重建 projection_positions。"""
        return self._projector.rebuild()

    # ------------------------------------------------------------------
    # Post-trade hooks
    # ------------------------------------------------------------------

    def _notify_trade(self, trade_info: dict) -> None:
        """通知所有注册的 on_trade 回调。"""
        for callback in self._on_trade:
            try:
                callback(trade_info)
            except Exception:
                pass  # hook 失败不影响交易本身
