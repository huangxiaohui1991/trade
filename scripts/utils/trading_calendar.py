#!/usr/bin/env python3
from typing import Optional, Union
"""
utils/trading_calendar.py — A股交易日历

判断某天是否为交易日（排除周末 + 法定节假日 + 调休）。

数据源优先级：
  1. akshare tool_trade_date_hist_sina() — 新浪历史交易日列表
  2. 本地缓存 data/cache/trading_calendar/
  3. 简单周末判断（fallback）

用法：
  from scripts.utils.trading_calendar import is_trading_day, next_trading_day
  is_trading_day()                    # 今天是否交易日
  is_trading_day("2026-04-11")        # 指定日期
  next_trading_day()                  # 下一个交易日
  trading_days_in_month("2026-04")    # 本月所有交易日
"""

import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from scripts.utils.cache import load_json_cache, save_json_cache
from scripts.utils.logger import get_logger

_logger = get_logger("utils.trading_calendar")

CACHE_KEY = "trade_dates"
CACHE_MAX_AGE = 7 * 24 * 3600  # 7 天刷新一次


def _parse_date(value) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _load_trade_dates_from_akshare() -> Optional[set[str]]:
    """从 akshare 拉取历史交易日列表"""
    try:
        import akshare as ak
        df = ak.tool_trade_date_hist_sina()
        if df is None or df.empty:
            return None
        dates = set()
        for _, row in df.iterrows():
            d = str(row.get("trade_date", "")).strip()
            if len(d) >= 10:
                dates.add(d[:10])
            elif hasattr(row.get("trade_date"), "strftime"):
                dates.add(row["trade_date"].strftime("%Y-%m-%d"))
        if len(dates) > 100:
            return dates
        return None
    except Exception as exc:
        _logger.info(f"[calendar] akshare 交易日历获取失败: {exc}")
        return None


def _load_cached_trade_dates() -> Optional[set[str]]:
    """从本地缓存读取"""
    try:
        cached = load_json_cache("trading_calendar", CACHE_KEY, max_age_seconds=CACHE_MAX_AGE)
        if cached and isinstance(cached.get("data"), list) and len(cached["data"]) > 100:
            return set(cached["data"])
    except Exception:
        pass
    return None


def _save_trade_dates_cache(dates: set[str]):
    """写入本地缓存"""
    try:
        save_json_cache("trading_calendar", CACHE_KEY, sorted(dates))
    except Exception as exc:
        _logger.warning(f"[calendar] 缓存写入失败: {exc}")


_trade_dates_cache: Optional[set[str]] = None


def _get_trade_dates() -> Optional[set[str]]:
    """获取交易日集合（带内存缓存）"""
    global _trade_dates_cache
    if _trade_dates_cache is not None:
        return _trade_dates_cache

    # 1. 本地文件缓存
    dates = _load_cached_trade_dates()
    if dates:
        _trade_dates_cache = dates
        return dates

    # 2. akshare 在线拉取
    dates = _load_trade_dates_from_akshare()
    if dates:
        _save_trade_dates_cache(dates)
        _trade_dates_cache = dates
        return dates

    return None


def is_trading_day(target=None) -> bool:
    """
    判断指定日期是否为 A 股交易日。

    Args:
        target: date / datetime / "YYYY-MM-DD" / None(今天)

    Returns:
        True = 交易日, False = 非交易日
    """
    if target is None:
        d = date.today()
    else:
        d = _parse_date(target)
        if d is None:
            return False

    # 周末一定不是交易日
    if d.weekday() >= 5:
        return False

    trade_dates = _get_trade_dates()
    if trade_dates is not None:
        return d.isoformat() in trade_dates

    # fallback: 只排除周末（无法判断节假日）
    return True


def next_trading_day(target=None) -> date:
    """返回 target 之后的下一个交易日（不含 target 本身）"""
    if target is None:
        d = date.today()
    else:
        d = _parse_date(target) or date.today()

    for _ in range(30):
        d += timedelta(days=1)
        if is_trading_day(d):
            return d
    # fallback: 跳过周末
    d = (_parse_date(target) or date.today()) + timedelta(days=1)
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d


def prev_trading_day(target=None) -> date:
    """返回 target 之前的上一个交易日（不含 target 本身）"""
    if target is None:
        d = date.today()
    else:
        d = _parse_date(target) or date.today()

    for _ in range(30):
        d -= timedelta(days=1)
        if is_trading_day(d):
            return d
    d = (_parse_date(target) or date.today()) - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def trading_days_in_range(start, end) -> list[str]:
    """返回 [start, end] 范围内的所有交易日（含两端）"""
    s = _parse_date(start)
    e = _parse_date(end)
    if not s or not e or s > e:
        return []
    result = []
    d = s
    while d <= e:
        if is_trading_day(d):
            result.append(d.isoformat())
        d += timedelta(days=1)
    return result


def trading_days_in_month(month_str: str) -> list[str]:
    """返回某月的所有交易日，month_str 格式 "YYYY-MM" """
    try:
        year, month = int(month_str[:4]), int(month_str[5:7])
    except (ValueError, IndexError):
        return []
    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        end = date(year, month + 1, 1) - timedelta(days=1)
    return trading_days_in_range(start, end)


def refresh_cache() -> dict:
    """强制刷新交易日历缓存"""
    global _trade_dates_cache
    _trade_dates_cache = None
    dates = _load_trade_dates_from_akshare()
    if dates:
        _save_trade_dates_cache(dates)
        _trade_dates_cache = dates
        return {"status": "ok", "count": len(dates), "source": "akshare"}
    return {"status": "error", "count": 0, "source": "none"}


if __name__ == "__main__":
    import json
    today = date.today()
    print(f"今天 {today.isoformat()}: {'交易日' if is_trading_day() else '非交易日'}")
    print(f"下一个交易日: {next_trading_day()}")
    print(f"上一个交易日: {prev_trading_day()}")
    month = today.strftime("%Y-%m")
    days = trading_days_in_month(month)
    print(f"{month} 共 {len(days)} 个交易日")
