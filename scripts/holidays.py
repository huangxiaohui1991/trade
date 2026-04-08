#!/usr/bin/env python3
"""
A股交易日历 — 节假日列表
每年初更新一次，数据来源：中国证监会公告

用法:
  from holidays import is_trading_day, next_trading_day
"""

from datetime import datetime, timedelta

# 2026年A股休市日（不含周末）
# 来源：证监会公告，每年12月底发布次年安排
HOLIDAYS_2026 = [
    # 元旦
    "2026-01-01", "2026-01-02",
    # 春节
    "2026-02-16", "2026-02-17", "2026-02-18", "2026-02-19", "2026-02-20",
    # 清明节
    "2026-04-04", "2026-04-05", "2026-04-06",
    # 劳动节
    "2026-05-01", "2026-05-02", "2026-05-03", "2026-05-04", "2026-05-05",
    # 端午节
    "2026-06-19",
    # 中秋节
    "2026-09-25",
    # 国庆节
    "2026-10-01", "2026-10-02", "2026-10-03", "2026-10-04",
    "2026-10-05", "2026-10-06", "2026-10-07",
]

# 2026年调休上班日（周末但开盘）
WORKDAYS_2026 = [
    "2026-02-14",  # 春节调休
    "2026-02-15",  # 春节调休
    "2026-04-07",  # 清明调休（注意：4月7日周二正常开盘，不需要调休）
    "2026-05-06",  # 劳动节调休
]

HOLIDAY_SET = set(HOLIDAYS_2026)
WORKDAY_SET = set(WORKDAYS_2026)


def is_trading_day(date=None):
    """判断是否为交易日"""
    if date is None:
        date = datetime.now()
    if isinstance(date, str):
        date = datetime.strptime(date, "%Y-%m-%d")

    date_str = date.strftime("%Y-%m-%d")

    # 节假日休市
    if date_str in HOLIDAY_SET:
        return False

    # 调休上班日
    if date_str in WORKDAY_SET:
        return True

    # 周末休市
    if date.weekday() >= 5:
        return False

    return True


def next_trading_day(date=None):
    """获取下一个交易日"""
    if date is None:
        date = datetime.now()
    if isinstance(date, str):
        date = datetime.strptime(date, "%Y-%m-%d")

    next_day = date + timedelta(days=1)
    while not is_trading_day(next_day):
        next_day += timedelta(days=1)
    return next_day


def prev_trading_day(date=None):
    """获取上一个交易日"""
    if date is None:
        date = datetime.now()
    if isinstance(date, str):
        date = datetime.strptime(date, "%Y-%m-%d")

    prev_day = date - timedelta(days=1)
    while not is_trading_day(prev_day):
        prev_day -= timedelta(days=1)
    return prev_day


if __name__ == "__main__":
    today = datetime.now()
    print(f"今天: {today.strftime('%Y-%m-%d')} | 交易日: {is_trading_day(today)}")
    print(f"下一个交易日: {next_trading_day(today).strftime('%Y-%m-%d')}")
    print(f"上一个交易日: {prev_trading_day(today).strftime('%Y-%m-%d')}")
