"""
execution/positions.py — 持仓投影（从 event_log 重建）

持仓状态 = f(position.* 事件序列)。
投影表只是缓存，可随时删除后从 event_log 重建。
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Optional

from hermes.execution.models import Position
from hermes.platform.events import EventStore


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class PositionManager:
    """持仓管理 — 事件化 + 投影同步。"""

    def __init__(self, event_store: EventStore, conn: sqlite3.Connection):
        self._events = event_store
        self._conn = conn

    def open_position(
        self,
        code: str,
        name: str,
        shares: int,
        avg_cost_cents: int,
        style: str,
        run_id: str,
        entry_day_low_cents: int = 0,
    ) -> Position:
        """开仓 → 追加 position.opened 事件 → 更新投影。"""
        now = _now_iso()
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        self._events.append(
            stream=f"position:{code}",
            stream_type="position",
            event_type="position.opened",
            payload={
                "code": code,
                "name": name,
                "shares": shares,
                "avg_cost_cents": avg_cost_cents,
                "style": style,
                "entry_day_low_cents": entry_day_low_cents,
            },
            metadata={"run_id": run_id},
        )

        pos = Position(
            code=code, name=name, style=style,
            shares=shares, avg_cost_cents=avg_cost_cents,
            entry_date=today,
            entry_day_low_cents=entry_day_low_cents,
            highest_since_entry_cents=avg_cost_cents,
            current_price_cents=avg_cost_cents,
            updated_at=now,
        )

        self._conn.execute(
            """INSERT OR REPLACE INTO projection_positions
               (code, name, style, shares, avg_cost_cents, entry_date,
                entry_day_low_cents, highest_since_entry_cents,
                current_price_cents, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (code, name, style, shares, avg_cost_cents, today,
             entry_day_low_cents, avg_cost_cents, avg_cost_cents, now),
        )

        return pos

    def close_position(
        self,
        code: str,
        shares: int,
        sell_price_cents: int,
        run_id: str,
        reason: str = "",
    ) -> int:
        """清仓 → 追加 position.closed 事件 → 删除投影。返回 realized_pnl_cents。"""
        row = self._conn.execute(
            "SELECT * FROM projection_positions WHERE code = ?", (code,)
        ).fetchone()
        if not row:
            raise ValueError(f"Position {code} not found")

        avg_cost_cents = row["avg_cost_cents"]
        holding_days = 0
        try:
            entry = datetime.strptime(row["entry_date"], "%Y-%m-%d")
            holding_days = (datetime.now(timezone.utc) - entry.replace(tzinfo=timezone.utc)).days
        except (ValueError, TypeError):
            pass

        realized_pnl_cents = (sell_price_cents - avg_cost_cents) * shares

        self._events.append(
            stream=f"position:{code}",
            stream_type="position",
            event_type="position.closed",
            payload={
                "code": code,
                "shares": shares,
                "sell_price_cents": sell_price_cents,
                "avg_cost_cents": avg_cost_cents,
                "realized_pnl_cents": realized_pnl_cents,
                "holding_days": holding_days,
                "reason": reason,
            },
            metadata={"run_id": run_id},
        )

        self._conn.execute("DELETE FROM projection_positions WHERE code = ?", (code,))
        return realized_pnl_cents

    def get_positions(self) -> list[Position]:
        """从投影表读取所有持仓。"""
        rows = self._conn.execute(
            "SELECT * FROM projection_positions ORDER BY entry_date"
        ).fetchall()
        return [self._row_to_position(r) for r in rows]

    def get_position(self, code: str) -> Optional[Position]:
        """从投影表读取单个持仓。"""
        row = self._conn.execute(
            "SELECT * FROM projection_positions WHERE code = ?", (code,)
        ).fetchone()
        return self._row_to_position(row) if row else None

    @staticmethod
    def _row_to_position(row: sqlite3.Row) -> Position:
        return Position(
            code=row["code"],
            name=row["name"],
            style=row["style"],
            shares=row["shares"],
            avg_cost_cents=row["avg_cost_cents"],
            entry_date=row["entry_date"],
            entry_day_low_cents=row["entry_day_low_cents"] or 0,
            stop_loss_cents=row["stop_loss_cents"] or 0,
            take_profit_cents=row["take_profit_cents"] or 0,
            highest_since_entry_cents=row["highest_since_entry_cents"] or 0,
            current_price_cents=row["current_price_cents"] or 0,
            unrealized_pnl_cents=row["unrealized_pnl_cents"] or 0,
            updated_at=row["updated_at"],
        )


class PositionProjector:
    """从 event_log 重建持仓投影。"""

    def __init__(self, event_store: EventStore, conn: sqlite3.Connection):
        self._events = event_store
        self._conn = conn

    def rebuild(self) -> list[Position]:
        """
        删除 projection_positions，从 event_log 完全重建。

        遍历所有 position.* 事件，按 stream 分组重放：
        - position.opened → 创建持仓
        - position.closed → 删除持仓
        """
        self._conn.execute("DELETE FROM projection_positions")

        # 查询所有 position 事件
        events = self._events.query(stream_type="position")

        # 按 stream 分组
        streams: dict[str, list[dict]] = {}
        for ev in events:
            s = ev["stream"]
            streams.setdefault(s, []).append(ev)

        positions: list[Position] = []

        for stream, evts in streams.items():
            # 按 version 排序
            evts.sort(key=lambda e: e.get("stream_version", 0))

            pos = None
            for ev in evts:
                et = ev["event_type"]
                p = ev["payload"]

                if et == "position.opened":
                    pos = Position(
                        code=p["code"],
                        name=p.get("name", p["code"]),
                        style=p.get("style", "unknown"),
                        shares=p["shares"],
                        avg_cost_cents=p["avg_cost_cents"],
                        entry_date=ev.get("occurred_at", "")[:10],
                        entry_day_low_cents=p.get("entry_day_low_cents", 0),
                        highest_since_entry_cents=p.get("avg_cost_cents", 0),
                        current_price_cents=p.get("avg_cost_cents", 0),
                        updated_at=ev.get("occurred_at", ""),
                    )
                elif et == "position.closed":
                    pos = None  # 已清仓

            if pos is not None:
                # 写入投影
                self._conn.execute(
                    """INSERT OR REPLACE INTO projection_positions
                       (code, name, style, shares, avg_cost_cents, entry_date,
                        entry_day_low_cents, highest_since_entry_cents,
                        current_price_cents, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (pos.code, pos.name, pos.style, pos.shares,
                     pos.avg_cost_cents, pos.entry_date,
                     pos.entry_day_low_cents, pos.highest_since_entry_cents,
                     pos.current_price_cents, pos.updated_at),
                )
                positions.append(pos)

        return positions
