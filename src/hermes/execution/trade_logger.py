"""
execution/trade_logger.py — 交易后自动日志

注册为 ExecutionService 的 on_trade 回调。
每次买入/卖出后自动：
1. 更新 Obsidian 持仓状态页
2. 写入交易日志到 Obsidian
3. 更新 report_artifacts
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Optional

from hermes.platform.events import EventStore
from hermes.reporting.obsidian import ObsidianProjector
from hermes.reporting.reports import ReportGenerator

_logger = logging.getLogger(__name__)


class TradeLogger:
    """交易后自动日志 — 注册为 on_trade 回调。"""

    def __init__(
        self,
        event_store: EventStore,
        conn: sqlite3.Connection,
        vault_path: Optional[str] = None,
    ):
        self._events = event_store
        self._conn = conn
        self._obsidian = ObsidianProjector(event_store, conn, vault_path) if vault_path else None
        self._reporter = ReportGenerator(event_store, conn)

    def __call__(self, trade_info: dict) -> None:
        """on_trade 回调入口。"""
        try:
            self._log_trade(trade_info)
        except Exception as e:
            _logger.warning(f"TradeLogger failed: {e}")

    def _log_trade(self, info: dict) -> None:
        side = info.get("side", "")
        code = info.get("code", "")
        name = info.get("name", code)
        shares = info.get("shares", 0)
        price = info.get("price_cents", 0) / 100
        run_id = info.get("run_id", "")

        _logger.info(f"[trade] {side} {name}({code}) {shares}股 @ ¥{price:.2f}")

        # 更新 Obsidian 持仓页
        if self._obsidian:
            try:
                self._obsidian.write_portfolio_status()
            except Exception as e:
                _logger.warning(f"Obsidian portfolio update failed: {e}")

            # 写交易日志
            try:
                today = datetime.now().strftime("%Y-%m-%d")
                now = datetime.now().strftime("%H:%M:%S")
                emoji = "🟢 买入" if side == "buy" else "🔴 卖出"
                log_entry = f"\n## {now} {emoji}\n\n- {name}({code}) {shares}股 @ ¥{price:.2f}\n"
                self._obsidian.write_daily_log(run_id, log_entry)
            except Exception as e:
                _logger.warning(f"Obsidian daily log failed: {e}")

        # 写 report_artifacts
        try:
            now_iso = datetime.now(timezone.utc).isoformat()
            self._conn.execute(
                """INSERT INTO report_artifacts
                   (artifact_id, run_id, report_type, format, content, delivered_to, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    f"trade_{code}_{now_iso[:19]}",
                    run_id,
                    "trade_log",
                    "text",
                    f"{side} {name}({code}) {shares}股 @ ¥{price:.2f}",
                    "",
                    now_iso,
                ),
            )
        except Exception as e:
            _logger.warning(f"Trade artifact write failed: {e}")
