"""Shared time helpers for audit timestamps and market-local business dates."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo


MARKET_TZ = ZoneInfo("Asia/Shanghai")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def local_now() -> datetime:
    return datetime.now(MARKET_TZ)


def local_now_iso() -> str:
    return local_now().isoformat()


def local_now_str(fmt: str = "%Y-%m-%d %H:%M") -> str:
    return local_now().strftime(fmt)


def local_today() -> date:
    return local_now().date()


def local_today_str() -> str:
    return local_today().isoformat()


def local_date_bounds_utc(target: date | str | None = None) -> tuple[str, str]:
    if target is None:
        target_date = local_today()
    elif isinstance(target, str):
        target_date = date.fromisoformat(target)
    else:
        target_date = target

    start_local = datetime.combine(target_date, time.min, tzinfo=MARKET_TZ)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc).isoformat(), end_local.astimezone(timezone.utc).isoformat()


def iso_to_local(iso_value: str) -> datetime:
    dt = datetime.fromisoformat(iso_value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(MARKET_TZ)


def iso_to_local_date_str(iso_value: str) -> str:
    return iso_to_local(iso_value).date().isoformat()
