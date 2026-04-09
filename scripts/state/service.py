#!/usr/bin/env python3
"""
Structured ledger-backed state service.

This module provides the single structured truth used by automation while
Obsidian remains the projection and review surface.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

try:
    import yaml
except ImportError:  # pragma: no cover - same behavior as config_loader
    yaml = None

from scripts.utils.config_loader import clear_config_cache, get_stocks
from scripts.utils.logger import get_logger
from scripts.utils.obsidian import ObsidianVault
from scripts.utils.parser import parse_md_table
from scripts.mx.mx_moni import MXMoni

LOGGER = get_logger("state.service")
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
LEDGER_DIR = PROJECT_ROOT / "data" / "ledger"
LEDGER_DB_PATH = LEDGER_DIR / "trade_state.sqlite3"
DB_ENV = "TRADE_STATE_DB_PATH"

AUTOMATED_RULES = [
    {"name": "dynamic_stop_loss", "mode": "automated"},
    {"name": "absolute_stop_loss", "mode": "automated"},
    {"name": "take_profit_batch_1", "mode": "automated"},
    {"name": "weekly_buy_limit", "mode": "automated"},
    {"name": "time_stop", "mode": "advisory"},
    {"name": "drawdown_take_profit", "mode": "advisory"},
]

PRIMARY_SCOPE = "cn_a_system"
SECONDARY_SCOPE = "hk_legacy"
PAPER_SCOPE = "paper_mx"
ALL_SCOPES = (PRIMARY_SCOPE, SECONDARY_SCOPE, PAPER_SCOPE)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS state_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS portfolio_positions (
  scope TEXT NOT NULL,
  code TEXT NOT NULL,
  name TEXT NOT NULL,
  market TEXT DEFAULT '',
  shares INTEGER NOT NULL DEFAULT 0,
  avg_cost REAL NOT NULL DEFAULT 0,
  current_price REAL NOT NULL DEFAULT 0,
  market_value REAL NOT NULL DEFAULT 0,
  status TEXT DEFAULT '',
  note TEXT DEFAULT '',
  source TEXT DEFAULT '',
  as_of_date TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  metadata_json TEXT DEFAULT '{}',
  PRIMARY KEY(scope, code)
);

CREATE TABLE IF NOT EXISTS portfolio_balances (
  scope TEXT PRIMARY KEY,
  cash_value REAL NOT NULL DEFAULT 0,
  total_capital REAL NOT NULL DEFAULT 0,
  total_market_value REAL NOT NULL DEFAULT 0,
  exposure REAL NOT NULL DEFAULT 0,
  source TEXT DEFAULT '',
  as_of_date TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  metadata_json TEXT DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS trade_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  external_id TEXT UNIQUE,
  scope TEXT NOT NULL,
  market TEXT DEFAULT '',
  code TEXT DEFAULT '',
  name TEXT DEFAULT '',
  side TEXT DEFAULT '',
  event_type TEXT DEFAULT '',
  shares INTEGER NOT NULL DEFAULT 0,
  price REAL NOT NULL DEFAULT 0,
  amount REAL NOT NULL DEFAULT 0,
  realized_pnl REAL NOT NULL DEFAULT 0,
  event_date TEXT NOT NULL,
  reason_code TEXT DEFAULT '',
  reason_text TEXT DEFAULT '',
  source TEXT DEFAULT '',
  metadata_json TEXT DEFAULT '{}',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  external_id TEXT UNIQUE,
  scope TEXT NOT NULL,
  broker TEXT DEFAULT '',
  broker_order_id TEXT DEFAULT '',
  code TEXT DEFAULT '',
  name TEXT DEFAULT '',
  side TEXT DEFAULT '',
  order_class TEXT DEFAULT '',
  order_type TEXT DEFAULT '',
  condition_type TEXT DEFAULT '',
  requested_shares INTEGER NOT NULL DEFAULT 0,
  filled_shares INTEGER NOT NULL DEFAULT 0,
  trigger_price REAL NOT NULL DEFAULT 0,
  limit_price REAL NOT NULL DEFAULT 0,
  avg_fill_price REAL NOT NULL DEFAULT 0,
  status TEXT DEFAULT '',
  confirm_status TEXT DEFAULT '',
  reason_code TEXT DEFAULT '',
  reason_text TEXT DEFAULT '',
  source TEXT DEFAULT '',
  placed_at TEXT DEFAULT '',
  triggered_at TEXT DEFAULT '',
  filled_at TEXT DEFAULT '',
  cancelled_at TEXT DEFAULT '',
  confirmed_at TEXT DEFAULT '',
  updated_at TEXT NOT NULL,
  metadata_json TEXT DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS pool_entries (
  code TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  bucket TEXT NOT NULL,
  total_score REAL NOT NULL DEFAULT 0,
  technical_score REAL NOT NULL DEFAULT 0,
  fundamental_score REAL NOT NULL DEFAULT 0,
  flow_score REAL NOT NULL DEFAULT 0,
  sentiment_score REAL NOT NULL DEFAULT 0,
  veto_triggered INTEGER NOT NULL DEFAULT 0,
  veto_signals_json TEXT DEFAULT '[]',
  passed_text TEXT DEFAULT '',
  note TEXT DEFAULT '',
  source TEXT DEFAULT '',
  added_date TEXT DEFAULT '',
  snapshot_date TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  metadata_json TEXT DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS market_snapshots (
  id INTEGER PRIMARY KEY CHECK(id = 1),
  signal TEXT NOT NULL,
  source TEXT DEFAULT '',
  source_chain_json TEXT DEFAULT '[]',
  as_of_date TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  detail_json TEXT NOT NULL
);
"""


def _now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def _today_str() -> str:
    return date.today().isoformat()


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _json_loads(value: str | None, default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except Exception:
        return default


def _db_path() -> Path:
    configured = os.environ.get(DB_ENV, "").strip()
    return Path(configured) if configured else LEDGER_DB_PATH


@contextmanager
def _connect():
    db_path = _db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def _meta_get(conn: sqlite3.Connection, key: str, default: str = "") -> str:
    row = conn.execute("SELECT value FROM state_meta WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def _meta_set(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO state_meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    if isinstance(value, (int, float)):
        return float(value)
    cleaned = str(value).strip()
    if not cleaned:
        return default
    negative = "亏" in cleaned and "-" not in cleaned
    cleaned = cleaned.replace(",", "")
    cleaned = re.sub(r"[^\d.\-]", "", cleaned)
    if cleaned in ("", "-", ".", "-."):
        return default
    try:
        number = float(cleaned)
    except ValueError:
        return default
    return -abs(number) if negative else number


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _extract_realized_pnl(value: Any, default: float = 0.0) -> float:
    text = str(value or "").strip()
    if not text:
        return default

    loss_match = re.search(r"亏[^0-9\-]*([0-9][0-9,]*(?:\.\d+)?)", text)
    if loss_match:
        return -abs(_safe_float(loss_match.group(1), default))

    gain_match = re.search(r"(?:盈|赚)[^0-9\-]*([0-9][0-9,]*(?:\.\d+)?)", text)
    if gain_match:
        return abs(_safe_float(gain_match.group(1), default))

    pnl_match = re.search(r"(?:pnl|profit)[^0-9\-]*([-+]?[0-9][0-9,]*(?:\.\d+)?)", text, re.IGNORECASE)
    if pnl_match:
        return _safe_float(pnl_match.group(1), default)

    return default


def _normalize_code(value: Any) -> str:
    if value is None:
        return ""
    code = str(value).strip()
    if not code:
        return ""
    if code.isdigit():
        return code.zfill(6)
    numeric = re.sub(r"\D", "", code)
    if numeric and len(numeric) <= 6 and numeric == code.replace(".", "").replace("HK", ""):
        return numeric.zfill(6)
    return code


def _parse_mmdd_date(value: str, fallback_year: int) -> str:
    raw = str(value).strip()
    if not raw:
        return _today_str()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", raw):
        return raw
    if re.match(r"^\d{2}-\d{2}$", raw):
        return f"{fallback_year}-{raw}"
    return raw


def _parse_stock_cell(cell: str) -> tuple[str, str]:
    raw = str(cell).strip()
    if not raw:
        return "", ""
    match = re.match(r"^(.*?)\(([\dA-Za-z.]+)\)$", raw)
    if match:
        return match.group(1).strip(), _normalize_code(match.group(2))
    return raw, ""


def _scope_from_record(market: str, record_type: str = "", note: str = "") -> str:
    combined = " ".join(str(item) for item in (market, record_type, note))
    if "模拟盘" in combined or "paper" in combined.lower():
        return PAPER_SCOPE
    if "港股" in combined or "遗留" in combined:
        return SECONDARY_SCOPE
    return PRIMARY_SCOPE


def _reason_code_from_text(side: str, reason: str, scope: str) -> str:
    text = str(reason or "")
    if "绝对止损" in text:
        return "absolute_stop_loss"
    if "动态止损" in text or ("止损" in text and "绝对" not in text):
        return "dynamic_stop_loss"
    if "止盈第一批" in text or "止盈1" in text:
        return "take_profit_batch_1"
    if "时间止损" in text:
        return "time_stop_advisory"
    if scope == PAPER_SCOPE and side == "buy":
        return "paper_buy"
    if scope == PAPER_SCOPE and side == "sell":
        return "paper_sell"
    if side == "buy":
        return "manual_buy"
    if side == "sell":
        return "manual_sell"
    return "manual_event"


def _recommendation_for_entry(entry: dict) -> str:
    if entry.get("veto_triggered"):
        return "❌"
    score = float(entry.get("total_score", 0) or 0)
    if score >= 7:
        return "✅"
    if score >= 5:
        return "🟡"
    return "❌"


def _load_md_rows(relative_path: str) -> list[dict]:
    vault = ObsidianVault()
    full_path = Path(vault.vault_path) / relative_path
    if not full_path.exists():
        return []
    content = full_path.read_text(encoding="utf-8")
    tables = parse_md_table(content)
    if not tables:
        return []
    return tables[0].get("rows", [])


def _ensure_bootstrapped(conn: sqlite3.Connection) -> None:
    if _meta_get(conn, "bootstrap_completed") == "1":
        return
    bootstrap_state(force=False)


def _write_balances(conn: sqlite3.Connection, balances: Iterable[dict]) -> None:
    conn.execute("DELETE FROM portfolio_balances")
    for balance in balances:
        conn.execute(
            """
            INSERT INTO portfolio_balances(
              scope, cash_value, total_capital, total_market_value, exposure,
              source, as_of_date, updated_at, metadata_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                balance["scope"],
                balance.get("cash_value", 0.0),
                balance.get("total_capital", 0.0),
                balance.get("total_market_value", 0.0),
                balance.get("exposure", 0.0),
                balance.get("source", "bootstrap"),
                balance.get("as_of_date", _today_str()),
                balance.get("updated_at", _now_ts()),
                _json_dumps(balance.get("metadata", {})),
            ),
        )


def _write_positions(conn: sqlite3.Connection, positions: Iterable[dict]) -> None:
    conn.execute("DELETE FROM portfolio_positions")
    for position in positions:
        conn.execute(
            """
            INSERT INTO portfolio_positions(
              scope, code, name, market, shares, avg_cost, current_price, market_value,
              status, note, source, as_of_date, updated_at, metadata_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                position["scope"],
                position["code"],
                position["name"],
                position.get("market", ""),
                position.get("shares", 0),
                position.get("avg_cost", 0.0),
                position.get("current_price", 0.0),
                position.get("market_value", 0.0),
                position.get("status", ""),
                position.get("note", ""),
                position.get("source", "bootstrap"),
                position.get("as_of_date", _today_str()),
                position.get("updated_at", _now_ts()),
                _json_dumps(position.get("metadata", {})),
            ),
        )


def _replace_portfolio_scope_snapshot(
    conn: sqlite3.Connection,
    scope: str,
    positions: Iterable[dict],
    balances: Iterable[dict],
) -> None:
    conn.execute("DELETE FROM portfolio_positions WHERE scope = ?", (scope,))
    conn.execute("DELETE FROM portfolio_balances WHERE scope = ?", (scope,))
    for position in positions:
        conn.execute(
            """
            INSERT INTO portfolio_positions(
              scope, code, name, market, shares, avg_cost, current_price, market_value,
              status, note, source, as_of_date, updated_at, metadata_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                position["scope"],
                position["code"],
                position["name"],
                position.get("market", ""),
                position.get("shares", 0),
                position.get("avg_cost", 0.0),
                position.get("current_price", 0.0),
                position.get("market_value", 0.0),
                position.get("status", ""),
                position.get("note", ""),
                position.get("source", "bootstrap"),
                position.get("as_of_date", _today_str()),
                position.get("updated_at", _now_ts()),
                _json_dumps(position.get("metadata", {})),
            ),
        )
    for balance in balances:
        conn.execute(
            """
            INSERT INTO portfolio_balances(
              scope, cash_value, total_capital, total_market_value, exposure,
              source, as_of_date, updated_at, metadata_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                balance["scope"],
                balance.get("cash_value", 0.0),
                balance.get("total_capital", 0.0),
                balance.get("total_market_value", 0.0),
                balance.get("exposure", 0.0),
                balance.get("source", "bootstrap"),
                balance.get("as_of_date", _today_str()),
                balance.get("updated_at", _now_ts()),
                _json_dumps(balance.get("metadata", {})),
            ),
        )


def _replace_trade_events(conn: sqlite3.Connection, events: Iterable[dict]) -> None:
    conn.execute("DELETE FROM trade_events")
    for event in events:
        _upsert_trade_event(conn, event)


def _upsert_trade_event(conn: sqlite3.Connection, event: dict) -> None:
    conn.execute(
        """
        INSERT INTO trade_events(
          external_id, scope, market, code, name, side, event_type, shares, price,
          amount, realized_pnl, event_date, reason_code, reason_text, source,
          metadata_json, created_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(external_id) DO UPDATE SET
          scope = excluded.scope,
          market = excluded.market,
          code = excluded.code,
          name = excluded.name,
          side = excluded.side,
          event_type = excluded.event_type,
          shares = excluded.shares,
          price = excluded.price,
          amount = excluded.amount,
          realized_pnl = excluded.realized_pnl,
          event_date = excluded.event_date,
          reason_code = excluded.reason_code,
          reason_text = excluded.reason_text,
          source = excluded.source,
          metadata_json = excluded.metadata_json,
          created_at = excluded.created_at
        """,
        (
            event.get("external_id"),
            event.get("scope", PRIMARY_SCOPE),
            event.get("market", ""),
            event.get("code", ""),
            event.get("name", ""),
            event.get("side", ""),
            event.get("event_type", event.get("side", "")),
            event.get("shares", 0),
            event.get("price", 0.0),
            event.get("amount", 0.0),
            event.get("realized_pnl", 0.0),
            event.get("event_date", _today_str()),
            event.get("reason_code", ""),
            event.get("reason_text", ""),
            event.get("source", "bootstrap"),
            _json_dumps(event.get("metadata", {})),
            event.get("created_at", _now_ts()),
        ),
    )


_ORDER_TERMINAL_STATUSES = {"filled", "cancelled", "reviewed", "exception"}


def _normalize_order_payload(order: dict, existing: dict | None = None) -> dict:
    existing = existing or {}
    metadata = existing.get("metadata", {})
    payload_metadata = order.get("metadata", {})
    if isinstance(metadata, dict) and isinstance(payload_metadata, dict):
        metadata = {**metadata, **payload_metadata}
    elif isinstance(payload_metadata, dict):
        metadata = payload_metadata

    external_id = str(
        order.get("external_id")
        or order.get("broker_order_id")
        or order.get("client_order_id")
        or existing.get("external_id", "")
    ).strip()
    if not external_id:
        raise ValueError("order requires external_id or broker_order_id")

    scope = str(order.get("scope", existing.get("scope", PRIMARY_SCOPE))).strip() or PRIMARY_SCOPE
    updated_at = str(order.get("updated_at", existing.get("updated_at", _now_ts()))).strip() or _now_ts()

    def pick(key: str, default: Any = "") -> Any:
        value = order.get(key, existing.get(key, default))
        if value in (None, "") and key in existing:
            return existing.get(key, default)
        return value if value is not None else default

    return {
        "external_id": external_id,
        "scope": scope,
        "broker": str(pick("broker", "")).strip(),
        "broker_order_id": str(pick("broker_order_id", order.get("external_id", ""))).strip(),
        "code": _normalize_code(pick("code", "")),
        "name": str(pick("name", "")).strip(),
        "side": str(pick("side", "")).strip(),
        "order_class": str(pick("order_class", "")).strip(),
        "order_type": str(pick("order_type", "")).strip(),
        "condition_type": str(pick("condition_type", "")).strip(),
        "requested_shares": _safe_int(pick("requested_shares", existing.get("requested_shares", 0))),
        "filled_shares": _safe_int(pick("filled_shares", existing.get("filled_shares", 0))),
        "trigger_price": _safe_float(pick("trigger_price", existing.get("trigger_price", 0.0))),
        "limit_price": _safe_float(pick("limit_price", existing.get("limit_price", 0.0))),
        "avg_fill_price": _safe_float(pick("avg_fill_price", existing.get("avg_fill_price", 0.0))),
        "status": str(pick("status", existing.get("status", "candidate"))).strip() or "candidate",
        "confirm_status": str(pick("confirm_status", existing.get("confirm_status", "not_required"))).strip() or "not_required",
        "reason_code": str(pick("reason_code", "")).strip(),
        "reason_text": str(pick("reason_text", "")).strip(),
        "source": str(pick("source", "runtime")).strip() or "runtime",
        "placed_at": str(pick("placed_at", existing.get("placed_at", updated_at))).strip(),
        "triggered_at": str(pick("triggered_at", existing.get("triggered_at", ""))).strip(),
        "filled_at": str(pick("filled_at", existing.get("filled_at", ""))).strip(),
        "cancelled_at": str(pick("cancelled_at", existing.get("cancelled_at", ""))).strip(),
        "confirmed_at": str(pick("confirmed_at", existing.get("confirmed_at", ""))).strip(),
        "updated_at": updated_at,
        "metadata": metadata,
    }


def _load_order_row(conn: sqlite3.Connection, external_id: str) -> dict | None:
    row = conn.execute("SELECT * FROM orders WHERE external_id = ?", (external_id,)).fetchone()
    if not row:
        return None
    order = dict(row)
    order["metadata"] = _json_loads(order.pop("metadata_json", "{}"), {})
    return order


def _order_rows(scope: str | None = None, status: str | None = None, conn: sqlite3.Connection | None = None) -> list[dict]:
    close_after = conn is None
    if close_after:
        context = _connect()
        conn = context.__enter__()
    try:
        query = "SELECT * FROM orders"
        params: list[Any] = []
        clauses = []
        if scope:
            clauses.append("scope = ?")
            params.append(scope)
        if status:
            clauses.append("status = ?")
            params.append(status)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY updated_at DESC, id DESC"
        rows = conn.execute(query, params).fetchall()
        orders = []
        for row in rows:
            order = dict(row)
            order["metadata"] = _json_loads(order.pop("metadata_json", "{}"), {})
            orders.append(order)
        return orders
    finally:
        if close_after:
            context.__exit__(None, None, None)


def upsert_order_state(order: dict, conn: sqlite3.Connection | None = None) -> dict:
    """Insert or update a structured order row without touching trade events."""
    own_conn = conn is None
    if own_conn:
        context = _connect()
        conn = context.__enter__()
    try:
        external_id = str(
            order.get("external_id")
            or order.get("broker_order_id")
            or order.get("client_order_id")
            or ""
        ).strip()
        existing = _load_order_row(conn, external_id) if external_id else None
        payload = _normalize_order_payload(order, existing)
        conn.execute(
            """
            INSERT INTO orders(
              external_id, scope, broker, broker_order_id, code, name, side, order_class,
              order_type, condition_type, requested_shares, filled_shares, trigger_price,
              limit_price, avg_fill_price, status, confirm_status, reason_code, reason_text,
              source, placed_at, triggered_at, filled_at, cancelled_at, confirmed_at,
              updated_at, metadata_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(external_id) DO UPDATE SET
              scope = excluded.scope,
              broker = excluded.broker,
              broker_order_id = excluded.broker_order_id,
              code = excluded.code,
              name = excluded.name,
              side = excluded.side,
              order_class = excluded.order_class,
              order_type = excluded.order_type,
              condition_type = excluded.condition_type,
              requested_shares = excluded.requested_shares,
              filled_shares = excluded.filled_shares,
              trigger_price = excluded.trigger_price,
              limit_price = excluded.limit_price,
              avg_fill_price = excluded.avg_fill_price,
              status = excluded.status,
              confirm_status = excluded.confirm_status,
              reason_code = excluded.reason_code,
              reason_text = excluded.reason_text,
              source = excluded.source,
              placed_at = excluded.placed_at,
              triggered_at = excluded.triggered_at,
              filled_at = excluded.filled_at,
              cancelled_at = excluded.cancelled_at,
              confirmed_at = excluded.confirmed_at,
              updated_at = excluded.updated_at,
              metadata_json = excluded.metadata_json
            """,
            (
                payload["external_id"],
                payload["scope"],
                payload["broker"],
                payload["broker_order_id"],
                payload["code"],
                payload["name"],
                payload["side"],
                payload["order_class"],
                payload["order_type"],
                payload["condition_type"],
                payload["requested_shares"],
                payload["filled_shares"],
                payload["trigger_price"],
                payload["limit_price"],
                payload["avg_fill_price"],
                payload["status"],
                payload["confirm_status"],
                payload["reason_code"],
                payload["reason_text"],
                payload["source"],
                payload["placed_at"],
                payload["triggered_at"],
                payload["filled_at"],
                payload["cancelled_at"],
                payload["confirmed_at"],
                payload["updated_at"],
                _json_dumps(payload["metadata"]),
            ),
        )
        return {**payload, "db_path": str(_db_path())}
    finally:
        if own_conn:
            context.__exit__(None, None, None)


def _project_stocks_yaml(snapshot: dict) -> str:
    if yaml is None:
        raise ImportError("PyYAML not installed")

    target = {
        "core_pool": [],
        "watch_pool": [],
        "blacklist": get_stocks().get("blacklist", {"permanent": [], "temporary": []}),
    }
    snapshot_date = snapshot.get("snapshot_date", _today_str())
    for bucket_key, bucket_name in (("core_pool", "core_pool"), ("watch_pool", "watch_pool")):
        entries = snapshot.get(bucket_name, [])
        for entry in entries:
            target[bucket_key].append(
                {
                    "code": str(entry.get("code", "")),
                    "name": entry.get("name", ""),
                    "added": entry.get("added_date") or snapshot_date,
                    "score": round(float(entry.get("total_score", 0) or 0), 1),
                }
            )

    config_path = PROJECT_ROOT / "config" / "stocks.yaml"
    with open(config_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(target, f, allow_unicode=True, sort_keys=False)
    clear_config_cache("stocks")
    return str(config_path)


def _render_pool_markdown(title: str, tags: list[str], entries: list[dict], snapshot_date: str) -> str:
    lines = [
        "---",
        f"date: {snapshot_date}",
        f"type: {'watchlist_core' if '核心池' in title else 'watchlist_observe'}",
        f"tags: [{', '.join(tags)}]",
        f"updated_at: {snapshot_date}",
        "---",
        "",
        f"# {title}",
        "",
        "> 自动投影自结构化账本（trade_state.sqlite3）",
        "",
        "| # | 股票 | 代码 | 四维总分 | 技术 | 基本面 | 资金 | 舆情 | 通过 | 备注 |",
        "|---|------|------|---------|------|--------|------|------|------|------|",
    ]
    if entries:
        for idx, entry in enumerate(entries, 1):
            lines.append(
                f"| {idx} | {entry.get('name', '')} | {entry.get('code', '')} | "
                f"{float(entry.get('total_score', 0) or 0):.1f} | "
                f"{float(entry.get('technical_score', 0) or 0):.1f} | "
                f"{float(entry.get('fundamental_score', 0) or 0):.1f} | "
                f"{float(entry.get('flow_score', 0) or 0):.1f} | "
                f"{float(entry.get('sentiment_score', 0) or 0):.1f} | "
                f"{entry.get('passed_text') or _recommendation_for_entry(entry)} | "
                f"{entry.get('note', '')} |"
            )
    else:
        lines.append("| — | — | — | — | — | — | — | — | — | 暂无 |")
    return "\n".join(lines) + "\n"


def _project_pool_markdown(snapshot: dict) -> dict:
    vault = ObsidianVault()
    snapshot_date = snapshot.get("snapshot_date", _today_str())

    core_content = _render_pool_markdown("核心池", ["核心池", "选股"], snapshot.get("core_pool", []), snapshot_date)
    watch_content = _render_pool_markdown("观察池", ["观察池", "选股"], snapshot.get("watch_pool", []), snapshot_date)

    core_path = Path(vault.vault_path) / "04-选股" / "核心池.md"
    watch_path = Path(vault.vault_path) / "04-选股" / "观察池.md"
    core_path.parent.mkdir(parents=True, exist_ok=True)
    watch_path.parent.mkdir(parents=True, exist_ok=True)
    core_path.write_text(core_content, encoding="utf-8")
    watch_path.write_text(watch_content, encoding="utf-8")
    return {"core_pool_path": str(core_path), "watch_pool_path": str(watch_path)}


def _bootstrap_pool_entries() -> list[dict]:
    stocks_cfg = get_stocks()
    core_rows = {str(row.get("代码", "")).strip(): row for row in _load_md_rows("04-选股/核心池.md")}
    watch_rows = {str(row.get("代码", "")).strip(): row for row in _load_md_rows("04-选股/观察池.md")}
    snapshot_date = _today_str()
    entries = []

    for bucket, items, md_rows in (
        ("core", stocks_cfg.get("core_pool", []), core_rows),
        ("watch", stocks_cfg.get("watch_pool", []), watch_rows),
    ):
        for item in items:
            code = _normalize_code(item.get("code", ""))
            if not code:
                continue
            md_row = md_rows.get(code, {})
            total_score = _safe_float(md_row.get("四维总分", item.get("score", 0)))
            entry = {
                "bucket": bucket,
                "code": code,
                "name": item.get("name", md_row.get("股票", code)),
                "total_score": round(total_score, 1),
                "technical_score": _safe_float(md_row.get("技术", 0)),
                "fundamental_score": _safe_float(md_row.get("基本面", 0)),
                "flow_score": _safe_float(md_row.get("资金", md_row.get("主力", 0))),
                "sentiment_score": _safe_float(md_row.get("舆情", 0)),
                "veto_triggered": False,
                "veto_signals": [],
                "passed_text": str(md_row.get("通过", _recommendation_for_entry({"total_score": total_score}))),
                "note": str(md_row.get("备注", "")).strip(),
                "source": "bootstrap",
                "added_date": item.get("added", snapshot_date),
                "snapshot_date": snapshot_date,
                "updated_at": _now_ts(),
                "metadata": {"bootstrap_source": "config+obsidian"},
            }
            entries.append(entry)
    return entries


def _bootstrap_portfolio_snapshot() -> tuple[list[dict], list[dict], str]:
    vault = ObsidianVault()
    portfolio = vault.read_portfolio()
    meta = portfolio.get("meta", {})
    account_overview = portfolio.get("account_overview", [])
    overview_map = {str(row.get("项目", "")).strip(): row for row in account_overview}
    as_of_date = str(meta.get("updated_at") or meta.get("date") or _today_str())
    updated_at = _now_ts()

    def build_position(row: dict, scope: str, market: str) -> dict | None:
        name = str(row.get("股票", "")).strip()
        code = _normalize_code(row.get("代码", ""))
        shares = _safe_int(row.get("持有股数", 0))
        if name in ("", "—", "空仓") or not code or code == "—" or shares <= 0:
            return None
        avg_cost = _safe_float(row.get("平均成本", row.get("成本", 0)))
        current_price = _safe_float(row.get("现价", row.get("最新价", avg_cost)))
        market_value = _safe_float(row.get("持仓市值", row.get("市值", current_price * shares)))
        return {
            "scope": scope,
            "code": code,
            "name": name,
            "market": market,
            "shares": shares,
            "avg_cost": avg_cost,
            "current_price": current_price,
            "market_value": market_value,
            "status": str(row.get("状态", row.get("操作", ""))).strip(),
            "note": str(row.get("备注", row.get("操作计划", ""))).strip(),
            "source": "bootstrap:portfolio",
            "as_of_date": as_of_date,
            "updated_at": updated_at,
            "metadata": row,
        }

    positions = []
    for row in portfolio.get("cn_a_holdings", []):
        position = build_position(row, PRIMARY_SCOPE, "CN_A")
        if position:
            positions.append(position)
    for row in portfolio.get("legacy_cn_holdings", []):
        position = build_position(row, SECONDARY_SCOPE, "CN_A_LEGACY")
        if position:
            positions.append(position)
    for row in portfolio.get("hk_legacy_holdings", []):
        position = build_position(row, SECONDARY_SCOPE, "HK")
        if position:
            positions.append(position)

    cn_market_value = sum(item["market_value"] for item in positions if item["scope"] == PRIMARY_SCOPE)
    hk_market_value = sum(item["market_value"] for item in positions if item["scope"] == SECONDARY_SCOPE)
    cn_cash_value = _safe_float(overview_map.get("可交易现金", {}).get("金额", meta.get("cash_value", 0)))
    cn_total_capital = _safe_float(meta.get("total_capital", 0)) or cn_cash_value + cn_market_value

    balances = [
        {
            "scope": PRIMARY_SCOPE,
            "cash_value": cn_cash_value,
            "total_capital": cn_total_capital,
            "total_market_value": cn_market_value,
            "exposure": (cn_market_value / cn_total_capital) if cn_total_capital else 0.0,
            "source": "bootstrap:portfolio",
            "as_of_date": as_of_date,
            "updated_at": updated_at,
            "metadata": {"account_overview": account_overview},
        },
        {
            "scope": SECONDARY_SCOPE,
            "cash_value": 0.0,
            "total_capital": hk_market_value,
            "total_market_value": hk_market_value,
            "exposure": 1.0 if hk_market_value else 0.0,
            "source": "bootstrap:portfolio",
            "as_of_date": as_of_date,
            "updated_at": updated_at,
            "metadata": {
                "legacy_cn_holdings": portfolio.get("legacy_cn_holdings", []),
                "hk_legacy_holdings": portfolio.get("hk_legacy_holdings", []),
            },
        },
        {
            "scope": PAPER_SCOPE,
            "cash_value": 0.0,
            "total_capital": 0.0,
            "total_market_value": 0.0,
            "exposure": 0.0,
            "source": "bootstrap:shadow",
            "as_of_date": as_of_date,
            "updated_at": updated_at,
            "metadata": {},
        },
    ]
    return positions, balances, as_of_date


def _mx_payload(result: Any) -> dict:
    if not isinstance(result, dict):
        return {}
    payload = result.get("data")
    return payload if isinstance(payload, dict) else result


def _build_paper_position(row: dict, as_of_date: str, updated_at: str) -> dict | None:
    name = str(row.get("stockName", row.get("secuName", row.get("name", "")))).strip()
    code = _normalize_code(row.get("stockCode", row.get("secuCode", row.get("code", ""))))
    shares = _safe_int(row.get("totalQty", row.get("currentQty", row.get("shares", 0))))
    if not code or shares <= 0:
        return None

    avg_cost = _safe_float(row.get("costPrice", row.get("avgCost", row.get("cost", 0))))
    current_price = _safe_float(row.get("lastPrice", row.get("currentPrice", row.get("price", 0))))
    market_value = _safe_float(row.get("marketValue", row.get("market_value", 0)))
    if market_value <= 0 and current_price > 0:
        market_value = round(current_price * shares, 2)
    if current_price <= 0 and market_value > 0 and shares > 0:
        current_price = round(market_value / shares, 4)

    return {
        "scope": PAPER_SCOPE,
        "code": code,
        "name": name or code,
        "market": str(row.get("market", row.get("marketType", "MX_PAPER"))).strip() or "MX_PAPER",
        "shares": shares,
        "avg_cost": avg_cost,
        "current_price": current_price,
        "market_value": market_value,
        "status": str(row.get("status", row.get("positionStatus", "持仓"))).strip(),
        "note": str(row.get("note", row.get("remark", ""))).strip(),
        "source": "broker:mx_moni",
        "as_of_date": as_of_date,
        "updated_at": updated_at,
        "metadata": row,
    }


def _paper_portfolio_snapshot() -> tuple[list[dict], list[dict], str]:
    mx = MXMoni()
    positions_result = _mx_payload(mx.positions())
    balance_result = _mx_payload(mx.balance())
    as_of_date = _today_str()
    updated_at = _now_ts()

    raw_positions = positions_result.get("posList", [])
    positions = []
    for row in raw_positions if isinstance(raw_positions, list) else []:
        if not isinstance(row, dict):
            continue
        position = _build_paper_position(row, as_of_date, updated_at)
        if position:
            positions.append(position)

    total_assets = _safe_float(balance_result.get("totalAssets", 0))
    cash_value = _safe_float(balance_result.get("availBalance", 0))
    total_market_value = _safe_float(balance_result.get("totalPosValue", 0))
    if total_assets <= 0:
        total_assets = cash_value + total_market_value

    balances = [
        {
            "scope": PAPER_SCOPE,
            "cash_value": cash_value,
            "total_capital": total_assets,
            "total_market_value": total_market_value,
            "exposure": (total_market_value / total_assets) if total_assets else 0.0,
            "source": "broker:mx_moni",
            "as_of_date": as_of_date,
            "updated_at": updated_at,
            "metadata": {
                "balance": balance_result,
                "positions": positions_result,
            },
        }
    ]
    return positions, balances, as_of_date


def _portfolio_weekly_trade_events(source: str = "bootstrap:weekly_record") -> list[dict]:
    vault = ObsidianVault()
    portfolio = vault.read_portfolio()
    meta = portfolio.get("meta", {})
    default_year = int(str(meta.get("date", _today_str()))[:4])
    events = []

    for idx, row in enumerate(portfolio.get("weekly_records", []), start=1):
        market = str(row.get("市场", "")).strip()
        record_type = str(row.get("类型", "")).strip()
        note = str(row.get("备注", "")).strip()
        scope = _scope_from_record(market, record_type, note)
        side_text = str(row.get("操作", "")).strip()
        side = "buy" if "买" in side_text else "sell" if "卖" in side_text else "other"
        stock_name, stock_code = _parse_stock_cell(row.get("股票", ""))
        event_date = _parse_mmdd_date(str(row.get("日期", "")).strip(), default_year)
        events.append(
            {
                "external_id": f"weekly:{event_date}:{scope}:{side}:{stock_code or stock_name}:{idx}",
                "scope": scope,
                "market": market,
                "code": stock_code,
                "name": stock_name,
                "side": side,
                "event_type": side,
                "shares": _safe_int(row.get("数量", 0)),
                "price": _safe_float(row.get("价格", 0)),
                "amount": _safe_float(row.get("金额", 0)),
                "realized_pnl": _extract_realized_pnl(note, 0.0),
                "event_date": event_date,
                "reason_code": _reason_code_from_text(side, note or record_type, scope),
                "reason_text": note or record_type,
                "source": source,
                "metadata": row,
                "created_at": _now_ts(),
            }
        )
    return events


def _bootstrap_trade_events() -> list[dict]:
    vault = ObsidianVault()
    events = _portfolio_weekly_trade_events(source="bootstrap:weekly_record")

    shadow_path = Path(vault.vault_path) / "03-复盘" / "模拟盘" / "交易记录.md"
    if shadow_path.exists():
        content = shadow_path.read_text(encoding="utf-8")
        tables = parse_md_table(content)
        rows = tables[0].get("rows", []) if tables else []
        for idx, row in enumerate(rows, start=1):
            raw_time = str(row.get("时间", "")).strip()
            event_date = raw_time[:10] if raw_time else _today_str()
            side_text = str(row.get("操作", "")).strip()
            side = "buy" if "买" in side_text else "sell" if "卖" in side_text else "other"
            reason_text = str(row.get("原因", "")).strip()
            events.append(
                {
                    "external_id": f"bootstrap:paper:{idx}:{event_date}:{row.get('代码', '')}",
                    "scope": PAPER_SCOPE,
                    "market": "MX_PAPER",
                    "code": _normalize_code(row.get("代码", "")),
                    "name": str(row.get("股票", "")).strip(),
                    "side": side,
                    "event_type": side,
                    "shares": _safe_int(row.get("数量", 0)),
                    "price": _safe_float(row.get("价格", 0)),
                    "amount": _safe_float(row.get("金额", 0)),
                    "realized_pnl": _extract_realized_pnl(reason_text, 0.0),
                    "event_date": event_date,
                    "reason_code": _reason_code_from_text(side, reason_text, PAPER_SCOPE),
                    "reason_text": reason_text,
                    "source": "bootstrap:shadow_trade_log",
                    "metadata": row,
                    "created_at": _now_ts(),
                }
            )
    return events


def bootstrap_state(force: bool = False) -> dict:
    """Seed the structured ledger from current Markdown/config projections."""
    with _connect() as conn:
        if not force and _meta_get(conn, "bootstrap_completed") == "1":
            return {
                "status": "skipped",
                "db_path": str(_db_path()),
                "reason": "already_bootstrapped",
            }

        positions, balances, as_of_date = _bootstrap_portfolio_snapshot()
        trade_events = _bootstrap_trade_events()
        pool_entries = _bootstrap_pool_entries()

        _write_positions(conn, positions)
        _write_balances(conn, balances)
        _replace_trade_events(conn, trade_events)
        save_pool_snapshot(pool_entries, metadata={"source": "bootstrap"}, conn=conn)

        _meta_set(conn, "bootstrap_completed", "1")
        _meta_set(conn, "bootstrap_at", _now_ts())
        _meta_set(conn, "bootstrap_date", as_of_date)
        _meta_set(conn, "rule_automation_scope", _json_dumps(AUTOMATED_RULES))

        return {
            "status": "success",
            "db_path": str(_db_path()),
            "positions": len(positions),
            "trade_events": len(trade_events),
            "pool_entries": len(pool_entries),
            "as_of_date": as_of_date,
        }


def sync_portfolio_state() -> dict:
    """Refresh structured portfolio balances/positions from portfolio.md."""
    positions, balances, as_of_date = _bootstrap_portfolio_snapshot()
    with _connect() as conn:
        _write_positions(conn, positions)
        _write_balances(conn, balances)
        _meta_set(conn, "portfolio_sync_at", _now_ts())
        _meta_set(conn, "portfolio_sync_date", as_of_date)
    return {
        "status": "success",
        "db_path": str(_db_path()),
        "positions": len(positions),
        "scopes": [balance.get("scope", "") for balance in balances],
        "as_of_date": as_of_date,
    }


def sync_activity_state() -> dict:
    """Refresh structured non-paper trade events from portfolio weekly records."""
    weekly_events = _portfolio_weekly_trade_events(source="sync:weekly_record")
    with _connect() as conn:
        _ensure_bootstrapped(conn)
        conn.execute(
            "DELETE FROM trade_events WHERE source IN (?, ?)",
            ("bootstrap:weekly_record", "sync:weekly_record"),
        )
        for event in weekly_events:
            _upsert_trade_event(conn, event)
        _meta_set(conn, "activity_sync_at", _now_ts())
        _meta_set(conn, "activity_sync_count", str(len(weekly_events)))

    counts = {
        PRIMARY_SCOPE: 0,
        SECONDARY_SCOPE: 0,
        PAPER_SCOPE: 0,
    }
    for event in weekly_events:
        scope = event.get("scope", PRIMARY_SCOPE)
        counts[scope] = counts.get(scope, 0) + 1
    return {
        "status": "success",
        "db_path": str(_db_path()),
        "imported_events": len(weekly_events),
        "counts_by_scope": counts,
    }


def _portfolio_rows(scope: str | None = None, conn: sqlite3.Connection | None = None) -> tuple[list[dict], list[dict]]:
    close_after = conn is None
    if close_after:
        context = _connect()
        conn = context.__enter__()
    try:
        _ensure_bootstrapped(conn)
        if scope:
            balance_rows = conn.execute(
                "SELECT * FROM portfolio_balances WHERE scope = ? ORDER BY scope",
                (scope,),
            ).fetchall()
            position_rows = conn.execute(
                "SELECT * FROM portfolio_positions WHERE scope = ? ORDER BY scope, code",
                (scope,),
            ).fetchall()
        else:
            balance_rows = conn.execute(
                "SELECT * FROM portfolio_balances ORDER BY scope"
            ).fetchall()
            position_rows = conn.execute(
                "SELECT * FROM portfolio_positions ORDER BY scope, code"
            ).fetchall()
        balances = [dict(row) for row in balance_rows]
        positions = [dict(row) for row in position_rows]
        for row in balances:
            row["metadata"] = _json_loads(row.pop("metadata_json", "{}"), {})
        for row in positions:
            row["metadata"] = _json_loads(row.pop("metadata_json", "{}"), {})
        return balances, positions
    finally:
        if close_after:
            context.__exit__(None, None, None)


def load_portfolio_snapshot(scope: str | None = None) -> dict:
    """Return the current structured portfolio snapshot."""
    if scope == PAPER_SCOPE:
        try:
            positions, balances, as_of_date = _paper_portfolio_snapshot()
            with _connect() as conn:
                _ensure_bootstrapped(conn)
                _replace_portfolio_scope_snapshot(conn, PAPER_SCOPE, positions, balances)
                _meta_set(conn, "paper_portfolio_sync_at", _now_ts())
                _meta_set(conn, "paper_portfolio_sync_date", as_of_date)
        except Exception as exc:
            LOGGER.warning(f"[state] paper portfolio snapshot refresh failed: {exc}")
    elif scope is None:
        try:
            positions, balances, as_of_date = _paper_portfolio_snapshot()
            with _connect() as conn:
                _ensure_bootstrapped(conn)
                _replace_portfolio_scope_snapshot(conn, PAPER_SCOPE, positions, balances)
                _meta_set(conn, "paper_portfolio_sync_at", _now_ts())
                _meta_set(conn, "paper_portfolio_sync_date", as_of_date)
        except Exception as exc:
            LOGGER.warning(f"[state] paper portfolio snapshot refresh failed: {exc}")
    balances, positions = _portfolio_rows(scope=scope)
    scopes = {}
    for balance in balances:
        scopes[balance["scope"]] = {
            "cash_value": balance["cash_value"],
            "total_capital": balance["total_capital"],
            "total_market_value": balance["total_market_value"],
            "exposure": balance["exposure"],
            "as_of_date": balance["as_of_date"],
            "source": balance.get("source", ""),
        }
    active_positions = [row for row in positions if _safe_int(row.get("shares", 0)) > 0]
    primary = scopes.get(PRIMARY_SCOPE, {})
    if scope == PAPER_SCOPE:
        paper = scopes.get(PAPER_SCOPE, {})
        return {
            "scope": PAPER_SCOPE,
            "as_of_date": paper.get("as_of_date", _today_str()),
            "positions": active_positions,
            "balances": balances,
            "summary": {
                "holding_count": len([row for row in active_positions if row.get("scope") == PAPER_SCOPE]),
                "current_exposure": round(paper.get("exposure", 0.0), 3),
                "cash_value": round(paper.get("cash_value", 0.0), 2),
                "total_capital": round(paper.get("total_capital", 0.0), 2),
                "scopes": scopes,
            },
        }
    return {
        "scope": scope or "all",
        "as_of_date": max((item.get("as_of_date", "") for item in balances), default=_today_str()),
        "positions": active_positions,
        "balances": balances,
        "summary": {
            "holding_count": len([row for row in active_positions if row.get("scope") == PRIMARY_SCOPE]),
            "current_exposure": round(primary.get("exposure", 0.0), 3),
            "cash_value": round(primary.get("cash_value", 0.0), 2),
            "total_capital": round(primary.get("total_capital", 0.0), 2),
            "scopes": scopes,
        },
    }


def load_order_snapshot(scope: str | None = None, status: str | None = None) -> dict:
    """Return the structured order snapshot without mutating any other table."""
    with _connect() as conn:
        orders = _order_rows(scope=scope, status=status, conn=conn)

    status_counts: dict[str, int] = {}
    scope_counts: dict[str, int] = {}
    class_counts: dict[str, int] = {}
    for order in orders:
        order_status = str(order.get("status", "")).strip() or "unknown"
        order_scope = str(order.get("scope", "")).strip() or "unknown"
        order_class = str(order.get("order_class", "")).strip() or "unknown"
        status_counts[order_status] = status_counts.get(order_status, 0) + 1
        scope_counts[order_scope] = scope_counts.get(order_scope, 0) + 1
        class_counts[order_class] = class_counts.get(order_class, 0) + 1

    terminal_count = sum(1 for order in orders if str(order.get("status", "")).strip() in _ORDER_TERMINAL_STATUSES)
    unresolved_count = len(orders) - terminal_count
    return {
        "scope": scope or "all",
        "status": status or "all",
        "orders": orders,
        "summary": {
            "order_count": len(orders),
            "open_count": unresolved_count,
            "terminal_count": terminal_count,
            "status_counts": status_counts,
            "scope_counts": scope_counts,
            "class_counts": class_counts,
        },
        "db_path": str(_db_path()),
    }


def _normalize_market_detail(detail: dict) -> dict:
    if not isinstance(detail, dict):
        return {}
    indices = detail.get("indices") if isinstance(detail.get("indices"), dict) else detail
    normalized = {}
    for name, raw in indices.items():
        if not isinstance(raw, dict):
            continue
        price = _safe_float(raw.get("price", raw.get("close", 0)))
        ma20 = _safe_float(raw.get("ma20", raw.get("MA20", 0)))
        ma60 = _safe_float(raw.get("ma60", raw.get("MA60", 0)))
        chg_pct = _safe_float(raw.get("chg_pct", raw.get("change_pct", 0)))
        above_ma20 = bool(raw.get("above_ma20", price >= ma20 if ma20 else False))
        below_ma60_days = _safe_int(raw.get("below_ma60_days", raw.get("ma60_days", 0)))
        normalized[name] = {
            "price": price,
            "chg_pct": chg_pct,
            "ma20": ma20,
            "ma60": ma60,
            "ma20_pct": ((price / ma20) - 1) * 100 if ma20 else 0,
            "ma60_pct": ((price / ma60) - 1) * 100 if ma60 else 0,
            "above_ma20": above_ma20,
            "ma60_days": below_ma60_days,
            "signal": "GREEN" if above_ma20 else "RED",
            "source": raw.get("source", ""),
            "source_chain": raw.get("source_chain", []),
        }
    return normalized


def save_market_snapshot(snapshot: dict) -> dict:
    with _connect() as conn:
        _meta_set(conn, "bootstrap_completed", _meta_get(conn, "bootstrap_completed", "0") or "0")
        conn.execute(
            """
            INSERT INTO market_snapshots(id, signal, source, source_chain_json, as_of_date, updated_at, detail_json)
            VALUES(1, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
              signal = excluded.signal,
              source = excluded.source,
              source_chain_json = excluded.source_chain_json,
              as_of_date = excluded.as_of_date,
              updated_at = excluded.updated_at,
              detail_json = excluded.detail_json
            """,
            (
                snapshot.get("signal", "CLEAR"),
                snapshot.get("source", ""),
                _json_dumps(snapshot.get("source_chain", [])),
                snapshot.get("as_of_date", _today_str()),
                snapshot.get("updated_at", _now_ts()),
                _json_dumps(snapshot.get("indices", {})),
            ),
        )
    return snapshot


def load_market_snapshot(refresh: bool = False) -> dict:
    """Return the latest unified market snapshot."""
    with _connect() as conn:
        if not refresh:
            row = conn.execute("SELECT * FROM market_snapshots WHERE id = 1").fetchone()
            if row and row["as_of_date"] == _today_str():
                return {
                    "as_of_date": row["as_of_date"],
                    "updated_at": row["updated_at"],
                    "signal": row["signal"],
                    "source": row["source"],
                    "source_chain": _json_loads(row["source_chain_json"], []),
                    "indices": _json_loads(row["detail_json"], {}),
                }
    from scripts.engine.market_timer import load_market_snapshot as load_engine_market_snapshot

    snapshot = dict(load_engine_market_snapshot())
    snapshot.setdefault("updated_at", _now_ts())
    snapshot.setdefault("as_of_date", _today_str())
    snapshot.setdefault("signal", snapshot.get("market_signal", "CLEAR"))
    snapshot.setdefault("market_signal", snapshot.get("signal", "CLEAR"))
    snapshot.setdefault("source", "market_timer")
    snapshot.setdefault("source_chain", [])
    snapshot.setdefault("indices", {})
    return save_market_snapshot(snapshot)


def save_pool_snapshot(entries: list[dict], metadata: dict | None = None, conn: sqlite3.Connection | None = None) -> dict:
    """Persist the latest pool snapshot and project it back to YAML/Obsidian."""
    own_conn = conn is None
    if own_conn:
        context = _connect()
        conn = context.__enter__()
    try:
        metadata = metadata or {}
        snapshot_date = metadata.get("snapshot_date", _today_str()) if metadata else _today_str()
        updated_at = _now_ts()
        conn.execute("DELETE FROM pool_entries")
        normalized_entries = []
        for entry in entries:
            bucket = str(entry.get("bucket", "avoid")).strip() or "avoid"
            code = _normalize_code(entry.get("code", ""))
            if not code:
                continue
            normalized = {
                "bucket": bucket,
                "code": code,
                "name": entry.get("name", code),
                "total_score": round(_safe_float(entry.get("total_score", 0)), 1),
                "technical_score": round(_safe_float(entry.get("technical_score", 0)), 1),
                "fundamental_score": round(_safe_float(entry.get("fundamental_score", 0)), 1),
                "flow_score": round(_safe_float(entry.get("flow_score", 0)), 1),
                "sentiment_score": round(_safe_float(entry.get("sentiment_score", 0)), 1),
                "veto_triggered": bool(entry.get("veto_triggered", False)),
                "veto_signals": list(entry.get("veto_signals", [])),
                "passed_text": entry.get("passed_text") or _recommendation_for_entry(entry),
                "note": str(entry.get("note", "")).strip(),
                "source": entry.get("source", metadata.get("source", "unknown")),
                "added_date": entry.get("added_date", snapshot_date),
                "snapshot_date": entry.get("snapshot_date", snapshot_date),
                "updated_at": entry.get("updated_at", updated_at),
                "metadata": entry.get("metadata", {}),
            }
            normalized_entries.append(normalized)
            conn.execute(
                """
                INSERT INTO pool_entries(
                  code, name, bucket, total_score, technical_score, fundamental_score,
                  flow_score, sentiment_score, veto_triggered, veto_signals_json,
                  passed_text, note, source, added_date, snapshot_date, updated_at, metadata_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized["code"],
                    normalized["name"],
                    normalized["bucket"],
                    normalized["total_score"],
                    normalized["technical_score"],
                    normalized["fundamental_score"],
                    normalized["flow_score"],
                    normalized["sentiment_score"],
                    1 if normalized["veto_triggered"] else 0,
                    _json_dumps(normalized["veto_signals"]),
                    normalized["passed_text"],
                    normalized["note"],
                    normalized["source"],
                    normalized["added_date"],
                    normalized["snapshot_date"],
                    normalized["updated_at"],
                    _json_dumps(normalized["metadata"]),
                ),
            )

        snapshot = {
            "snapshot_date": snapshot_date,
            "updated_at": updated_at,
            "source": metadata.get("source", "unknown"),
            "metadata": metadata,
            "core_pool": [item for item in normalized_entries if item["bucket"] == "core"],
            "watch_pool": [item for item in normalized_entries if item["bucket"] == "watch"],
            "other_entries": [item for item in normalized_entries if item["bucket"] not in {"core", "watch"}],
        }
        snapshot["entries"] = normalized_entries
        snapshot["summary"] = {
            "core_count": len(snapshot["core_pool"]),
            "watch_count": len(snapshot["watch_pool"]),
            "other_count": len(snapshot["other_entries"]),
        }
        projection_paths = {}
        if own_conn:
            projection_paths["stocks_yaml_path"] = _project_stocks_yaml(snapshot)
            try:
                vault = ObsidianVault()
                projection_paths.update(vault.sync_pool_projection(
                    normalized_entries,
                    {
                        **metadata,
                        "source": snapshot["source"],
                        "updated_at": snapshot_date,
                    },
                ))
            except Exception as exc:
                LOGGER.warning(f"[state] pool projection sync failed: {exc}")
            _meta_set(conn, "pool_projection_paths", _json_dumps(projection_paths))
            _meta_set(conn, "pool_snapshot_meta", _json_dumps({"snapshot_date": snapshot_date, **metadata}))
        snapshot["projection_paths"] = projection_paths
        snapshot["db_path"] = str(_db_path())
        return snapshot
    finally:
        if own_conn:
            context.__exit__(None, None, None)


def load_pool_snapshot() -> dict:
    """Return the latest structured pool snapshot."""
    with _connect() as conn:
        _ensure_bootstrapped(conn)
        rows = conn.execute(
            "SELECT * FROM pool_entries ORDER BY CASE bucket WHEN 'core' THEN 0 WHEN 'watch' THEN 1 ELSE 2 END, total_score DESC, code"
        ).fetchall()
        entries = []
        for row in rows:
            entry = dict(row)
            entry["veto_triggered"] = bool(entry["veto_triggered"])
            entry["veto_signals"] = _json_loads(entry.pop("veto_signals_json", "[]"), [])
            entry["metadata"] = _json_loads(entry.pop("metadata_json", "{}"), {})
            entries.append(entry)
        meta = _json_loads(_meta_get(conn, "pool_snapshot_meta", "{}"), {})
    core_pool = [entry for entry in entries if entry["bucket"] == "core"]
    watch_pool = [entry for entry in entries if entry["bucket"] == "watch"]
    other_entries = [entry for entry in entries if entry["bucket"] not in {"core", "watch"}]
    return {
        "snapshot_date": meta.get("snapshot_date", _today_str()),
        "updated_at": max((entry.get("updated_at", "") for entry in entries), default=""),
        "source": meta.get("source", ""),
        "metadata": meta,
        "entries": entries,
        "core_pool": core_pool,
        "watch_pool": watch_pool,
        "other_entries": other_entries,
        "summary": {
            "core_count": len(core_pool),
            "watch_count": len(watch_pool),
            "other_count": len(other_entries),
        },
    }


def record_trade_event(event: dict) -> dict:
    """Append one structured trade event."""
    payload = {
        "external_id": event.get("external_id"),
        "scope": event.get("scope", PRIMARY_SCOPE),
        "market": event.get("market", ""),
        "code": _normalize_code(event.get("code", "")),
        "name": event.get("name", ""),
        "side": event.get("side", ""),
        "event_type": event.get("event_type", event.get("side", "")),
        "shares": _safe_int(event.get("shares", 0)),
        "price": _safe_float(event.get("price", 0)),
        "amount": _safe_float(event.get("amount", 0)),
        "realized_pnl": _safe_float(event.get("realized_pnl", 0)),
        "event_date": event.get("event_date", _today_str()),
        "reason_code": event.get("reason_code") or _reason_code_from_text(
            event.get("side", ""), event.get("reason_text", ""), event.get("scope", PRIMARY_SCOPE)
        ),
        "reason_text": event.get("reason_text", ""),
        "source": event.get("source", "runtime"),
        "metadata": event.get("metadata", {}),
        "created_at": event.get("created_at", _now_ts()),
    }
    with _connect() as conn:
        _ensure_bootstrapped(conn)
        _upsert_trade_event(conn, payload)
    return payload


def load_activity_summary(window: str | int = "week", scope: str = PRIMARY_SCOPE) -> dict:
    """Return structured trade activity for the requested scope and time window."""
    end_date = date.today()
    if window == "week":
        start_date = end_date - timedelta(days=end_date.weekday())
    elif isinstance(window, int):
        start_date = end_date - timedelta(days=max(window - 1, 0))
    else:
        start_date = end_date - timedelta(days=6)

    with _connect() as conn:
        _ensure_bootstrapped(conn)
        rows = conn.execute(
            """
            SELECT * FROM trade_events
            WHERE scope = ? AND event_date >= ? AND event_date <= ?
            ORDER BY event_date ASC, id ASC
            """,
            (scope, start_date.isoformat(), end_date.isoformat()),
        ).fetchall()

    events = []
    for row in rows:
        event = dict(row)
        event["metadata"] = _json_loads(event.pop("metadata_json", "{}"), {})
        events.append(event)

    buy_events = [event for event in events if event.get("side") == "buy"]
    sell_events = [event for event in events if event.get("side") == "sell"]
    return {
        "scope": scope,
        "window": window,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "trade_events": events,
        "buy_count": len(buy_events),
        "sell_count": len(sell_events),
        "weekly_buy_count": len(buy_events),
        "weekly_sell_count": len(sell_events),
        "weekly_buys": len(buy_events),
        "trade_count": len(events),
        "total_trades": len(buy_events) + len(sell_events),
        "realized_pnl": round(sum(_safe_float(event.get("realized_pnl", 0)) for event in sell_events), 2),
        "source": "structured_ledger",
    }


def audit_state() -> dict:
    """Check whether pool projections are in sync with the structured ledger."""
    snapshot = load_pool_snapshot()
    expected = {
        "core": {
            entry["code"]: round(float(entry.get("total_score", 0) or 0), 1)
            for entry in snapshot.get("core_pool", [])
        },
        "watch": {
            entry["code"]: round(float(entry.get("total_score", 0) or 0), 1)
            for entry in snapshot.get("watch_pool", [])
        },
    }

    stocks_cfg = get_stocks()
    config_view = {
        "core": {
            _normalize_code(item.get("code", "")): round(_safe_float(item.get("score", 0)), 1)
            for item in stocks_cfg.get("core_pool", [])
        },
        "watch": {
            _normalize_code(item.get("code", "")): round(_safe_float(item.get("score", 0)), 1)
            for item in stocks_cfg.get("watch_pool", [])
        },
    }
    def _filtered_md_scores(relative_path: str) -> dict:
        scores = {}
        for row in _load_md_rows(relative_path):
            raw_code = str(row.get("代码", "")).strip()
            code = _normalize_code(raw_code)
            if not code or code in {"—", "-", "N/A"}:
                continue
            scores[code] = round(_safe_float(row.get("四维总分", row.get("总分", 0)), 0.0), 1)
        return scores

    md_view = {
        "core": _filtered_md_scores("04-选股/核心池.md"),
        "watch": _filtered_md_scores("04-选股/观察池.md"),
    }

    checks = {}
    overall_ok = True
    for label, view in (("stocks_yaml", config_view), ("obsidian_projection", md_view)):
        mismatches = []
        for bucket in ("core", "watch"):
            if view[bucket] != expected[bucket]:
                mismatches.append(
                    {
                        "bucket": bucket,
                        "expected": expected[bucket],
                        "actual": view[bucket],
                    }
                )
        checks[label] = {"ok": not mismatches, "mismatches": mismatches}
        overall_ok = overall_ok and not mismatches

    return {
        "status": "ok" if overall_ok else "drift",
        "snapshot_date": snapshot.get("snapshot_date", _today_str()),
        "checks": checks,
    }
