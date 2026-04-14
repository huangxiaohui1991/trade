"""
reporting/projectors.py — 投影更新器

从 event_log 同步更新所有 projection 表。
reporting 只读消费事实，不反写业务表。
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Optional

from hermes.platform.events import EventStore


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class ProjectionUpdater:
    """从 event_log 同步更新所有 projection 表。"""

    def __init__(self, event_store: EventStore, conn: sqlite3.Connection):
        self._events = event_store
        self._conn = conn

    def rebuild_all(self) -> dict:
        """删除所有 projection 数据，从 event_log 完全重建。"""
        stats = {}
        stats["positions"] = self._rebuild_positions()
        stats["orders"] = self._rebuild_orders()
        return stats

    def sync_all(self, since: Optional[str] = None) -> dict:
        """增量同步（简化版：目前等同于 rebuild）。"""
        return self.rebuild_all()

    # ------------------------------------------------------------------
    # Positions
    # ------------------------------------------------------------------

    def _rebuild_positions(self) -> int:
        """从 position.* 事件重建 projection_positions。"""
        self._conn.execute("DELETE FROM projection_positions")

        events = self._events.query(stream_type="position")
        streams: dict[str, list[dict]] = {}
        for ev in events:
            streams.setdefault(ev["stream"], []).append(ev)

        count = 0
        for stream, evts in streams.items():
            evts.sort(key=lambda e: e.get("stream_version", 0))

            pos_data = None
            for ev in evts:
                et = ev["event_type"]
                p = ev["payload"]

                if et == "position.opened":
                    pos_data = {
                        "code": p["code"],
                        "name": p.get("name", p["code"]),
                        "style": p.get("style", "unknown"),
                        "shares": p["shares"],
                        "avg_cost_cents": p["avg_cost_cents"],
                        "entry_date": ev.get("occurred_at", "")[:10],
                        "entry_day_low_cents": p.get("entry_day_low_cents", 0),
                        "highest_since_entry_cents": p.get("avg_cost_cents", 0),
                        "current_price_cents": p.get("avg_cost_cents", 0),
                        "updated_at": ev.get("occurred_at", ""),
                    }
                elif et == "position.closed":
                    pos_data = None

            if pos_data:
                self._conn.execute(
                    """INSERT OR REPLACE INTO projection_positions
                       (code, name, style, shares, avg_cost_cents, entry_date,
                        entry_day_low_cents, highest_since_entry_cents,
                        current_price_cents, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (pos_data["code"], pos_data["name"], pos_data["style"],
                     pos_data["shares"], pos_data["avg_cost_cents"],
                     pos_data["entry_date"], pos_data["entry_day_low_cents"],
                     pos_data["highest_since_entry_cents"],
                     pos_data["current_price_cents"], pos_data["updated_at"]),
                )
                count += 1

        return count

    # ------------------------------------------------------------------
    # Orders
    # ------------------------------------------------------------------

    def _rebuild_orders(self) -> int:
        """从 order.* 事件重建 projection_orders。"""
        self._conn.execute("DELETE FROM projection_orders")

        events = self._events.query(stream_type="order")
        streams: dict[str, list[dict]] = {}
        for ev in events:
            streams.setdefault(ev["stream"], []).append(ev)

        count = 0
        for stream, evts in streams.items():
            evts.sort(key=lambda e: e.get("stream_version", 0))

            order_data = None
            for ev in evts:
                et = ev["event_type"]
                p = ev["payload"]

                if et == "order.created":
                    order_data = {
                        "order_id": p["order_id"],
                        "code": p["code"],
                        "side": p["side"],
                        "shares": p["shares"],
                        "price_cents": p["price_cents"],
                        "status": "pending",
                        "broker": p.get("broker", ""),
                        "created_at": ev.get("occurred_at", ""),
                        "filled_at": None,
                        "updated_at": ev.get("occurred_at", ""),
                    }
                elif et == "order.filled" and order_data:
                    order_data["status"] = "filled"
                    order_data["filled_at"] = ev.get("occurred_at", "")
                    order_data["updated_at"] = ev.get("occurred_at", "")
                elif et == "order.cancelled" and order_data:
                    order_data["status"] = "cancelled"
                    order_data["updated_at"] = ev.get("occurred_at", "")

            if order_data:
                self._conn.execute(
                    """INSERT OR REPLACE INTO projection_orders
                       (order_id, code, side, shares, price_cents, status,
                        broker, created_at, filled_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (order_data["order_id"], order_data["code"],
                     order_data["side"], order_data["shares"],
                     order_data["price_cents"], order_data["status"],
                     order_data["broker"], order_data["created_at"],
                     order_data["filled_at"], order_data["updated_at"]),
                )
                count += 1

        return count

    # ------------------------------------------------------------------
    # Market State
    # ------------------------------------------------------------------

    def sync_market_state(self, index_data: dict[str, dict]) -> int:
        """从指数数据同步 projection_market_state。"""
        now = _now_iso()
        count = 0

        for name, data in index_data.items():
            if "error" in data:
                continue
            symbol = data.get("symbol", name)
            self._conn.execute(
                """INSERT OR REPLACE INTO projection_market_state
                   (index_symbol, name, signal, price_cents, change_pct,
                    ma20_pct, ma60_pct, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    symbol, name,
                    data.get("signal", ""),
                    int(data.get("close", 0) * 100) if data.get("close") else None,
                    data.get("change_pct"),
                    data.get("ma20_pct"),
                    data.get("ma60_pct"),
                    now,
                ),
            )
            count += 1

        return count

    # ------------------------------------------------------------------
    # Candidate Pool
    # ------------------------------------------------------------------

    def sync_candidate_pool(self, entries: list[dict]) -> int:
        """从评分结果同步 projection_candidate_pool。"""
        now = _now_iso()
        count = 0

        for entry in entries:
            code = entry.get("code", "")
            if not code:
                continue
            tier = entry.get("pool_tier", entry.get("bucket", "watch"))
            self._conn.execute(
                """INSERT OR REPLACE INTO projection_candidate_pool
                   (code, pool_tier, name, score, added_at, last_scored_at,
                    streak_days, note)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    code, tier,
                    entry.get("name", ""),
                    entry.get("score", entry.get("total_score")),
                    entry.get("added_at", now[:10]),
                    now[:10],
                    entry.get("streak_days", 0),
                    entry.get("note", ""),
                ),
            )
            count += 1

        return count

    # ------------------------------------------------------------------
    # Balances
    # ------------------------------------------------------------------

    def sync_balances(
        self,
        scope: str,
        cash_cents: int,
        total_asset_cents: int,
        weekly_buy_count: int = 0,
        daily_pnl_cents: int = 0,
        consecutive_loss_days: int = 0,
    ) -> None:
        """同步 projection_balances。"""
        now = _now_iso()
        self._conn.execute(
            """INSERT OR REPLACE INTO projection_balances
               (scope, cash_cents, total_asset_cents, weekly_buy_count,
                daily_pnl_cents, consecutive_loss_days, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (scope, cash_cents, total_asset_cents, weekly_buy_count,
             daily_pnl_cents, consecutive_loss_days, now),
        )
