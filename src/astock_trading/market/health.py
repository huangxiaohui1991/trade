"""
market/health.py — 数据源健康聚合。

基于 market_observations 的最近观测时间做轻量健康判断。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


@dataclass(frozen=True)
class DataSourceExpectation:
    name: str
    kinds: tuple[str, ...]
    max_age_hours: int
    required: bool = False
    min_payload_count: int = 1


DEFAULT_EXPECTATIONS = (
    DataSourceExpectation("hot_stocks", ("hot_stocks",), 24, True),
    DataSourceExpectation("northbound_realtime", ("northbound_realtime",), 24, True),
    DataSourceExpectation("baidu_fund_flow", ("fund_flow", "flow"), 24, True),
    DataSourceExpectation("industry_comparison", ("industry_comparison",), 72, False),
    DataSourceExpectation("announcements", ("announcements",), 72, False),
    DataSourceExpectation("research_reports", ("research_reports",), 168, False),
    DataSourceExpectation("stock_news", ("stock_news",), 72, False),
    DataSourceExpectation("basic_info", ("basic_info",), 168, False),
)


def _parse_dt(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _payload_count(payload_json: Optional[str]) -> int:
    if not payload_json:
        return 0
    try:
        payload = json.loads(payload_json)
    except json.JSONDecodeError:
        return 0
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        for key in ("items", "records", "data", "upcoming"):
            value = payload.get(key)
            if isinstance(value, list):
                return len(value)
        total = payload.get("total")
        if isinstance(total, int):
            return total
        return len(payload)
    return 1


def _latest_for_kinds(conn, kinds: tuple[str, ...]) -> Optional[dict]:
    placeholders = ",".join("?" for _ in kinds)
    row = conn.execute(
        f"""SELECT source, kind, symbol, observed_at, payload_json
            FROM market_observations
            WHERE kind IN ({placeholders})
            ORDER BY observed_at DESC
            LIMIT 1""",
        kinds,
    ).fetchone()
    return dict(row) if row else None


def evaluate_data_source_health(
    conn,
    *,
    now: Optional[datetime] = None,
    max_age_hours: Optional[int] = None,
    expectations: tuple[DataSourceExpectation, ...] = DEFAULT_EXPECTATIONS,
) -> dict:
    """汇总数据源健康状态。

    required 源缺失或过期时整体 failed；optional 源缺失或过期时 warning。
    """
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    checks: dict[str, dict] = {}
    required_missing: list[str] = []
    optional_missing: list[str] = []

    for expected in expectations:
        latest = _latest_for_kinds(conn, expected.kinds)
        age_limit = max_age_hours or expected.max_age_hours
        if not latest:
            item = {
                "status": "down",
                "required": expected.required,
                "latest_observed_at": None,
                "age_hours": None,
                "max_age_hours": age_limit,
                "source": "",
                "kind": ",".join(expected.kinds),
                "symbol": "",
                "payload_count": 0,
                "min_payload_count": expected.min_payload_count,
            }
        else:
            observed = _parse_dt(latest["observed_at"])
            age_hours = (now - observed).total_seconds() / 3600
            payload_count = _payload_count(latest["payload_json"])
            item = {
                "status": "healthy"
                if age_hours <= age_limit and payload_count >= expected.min_payload_count
                else "degraded",
                "required": expected.required,
                "latest_observed_at": latest["observed_at"],
                "age_hours": round(age_hours, 2),
                "max_age_hours": age_limit,
                "source": latest["source"],
                "kind": latest["kind"],
                "symbol": latest["symbol"],
                "payload_count": payload_count,
                "min_payload_count": expected.min_payload_count,
            }

        checks[expected.name] = item
        if item["status"] != "healthy":
            if expected.required:
                required_missing.append(expected.name)
            else:
                optional_missing.append(expected.name)

    status = "failed" if required_missing else "warning" if optional_missing else "ok"
    return {
        "status": status,
        "checks": checks,
        "required_missing": required_missing,
        "optional_missing": optional_missing,
    }
