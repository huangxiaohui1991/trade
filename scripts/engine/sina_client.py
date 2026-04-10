#!/usr/bin/env python3
"""
engine/sina_client.py — 新浪/腾讯实时行情客户端

优先级：
  1. 新浪实时 API（免费，无需 key，稳定）
  2. 腾讯实时 API（免费，无需 key，备用）
  3. akshare 东财（作为最终兜底）

用于：
  - get_realtime_sina(codes): 单次实时行情
  - get_realtime_batch_sina(codes): 批量实时行情（一次请求多股）
"""

import os
import re
import sys
import time
import logging
import warnings
from typing import Optional, Dict, Any, List

warnings.filterwarnings("ignore")

import httpx

try:
    from scripts.engine.technical import normalize_code, to_sina_symbol
except ImportError:
    def normalize_code(code): 
        code = str(code).strip()
        if len(code) == 6:
            if code[0] in ('6', '5', '9'): return code
            else: return code
        return code
    def to_sina_symbol(code):
        code = normalize_code(code)
        if code[0] in ('6', '5', '9'): return f"sh{code}"
        return f"sz{code}"

_logger = logging.getLogger("sina_client")

# 新浪实时行情（批量）
SINA_BATCH_URL = "https://hq.sinajs.cn/list={codes}"

# 腾讯实时行情（批量）
TENCENT_BATCH_URL = "https://qt.gtimg.cn/q={codes}"


def _parse_sina_batch(text: str, codes: list) -> Dict[str, Dict[str, Any]]:
    """
    解析新浪批量响应
    text 形如：var hq_str_sh600519="贵州茅台,1800.00,1805.00,1800.00,1809.00,1795.00,1800.00,1800.00,100,...";
    返回 {code: {price, change_pct, volume, ...}}
    """
    results = {}
    lines = text.strip().split("\n")
    code_map = {normalize_code(c): c for c in codes}
    hq_map = {}

    for line in lines:
        m = re.search(r'hq_str_(\w+)="([^"]*)"', line)
        if not m:
            continue
        sym = m.group(1)  # sh600519
        vals = m.group(2).split(",")
        if len(vals) < 32:
            continue
        # 从 sym 提取纯代码
        raw_code = sym[2:] if sym.startswith(("sh", "sz")) else sym
        code = code_map.get(raw_code, raw_code)

        try:
            # 新浪字段顺序：[0]=name [1]=今开 [2]=昨收 [3]=当前价 [4]=最高 [5]=最低 [8]=成交量(手) [9]=成交额(元)
            open_price = float(vals[1]) if vals[1] else 0
            prev_close = float(vals[2]) if vals[2] else 0
            current = float(vals[3]) if vals[3] else 0
            high = float(vals[4]) if vals[4] else 0
            low = float(vals[5]) if vals[5] else 0
            volume = int(float(vals[8])) if vals[8] else 0  # 手→股
            amount = float(vals[9]) if vals[9] else 0
            change_pct = 0.0
            if prev_close and prev_close != 0:
                change_pct = round((current - prev_close) / prev_close * 100, 2)
            change_amount = round(current - prev_close, 2) if current else 0

            # PE/市值：新浪实时数据不含，标记为 None（由财务接口补充）
            results[code] = {
                "code": code,
                "name": vals[0],
                "price": current,
                "prev_close": prev_close,
                "open": open_price,
                "high": high,
                "low": low,
                "volume": volume * 100,  # 转为股
                "amount": amount,
                "change_pct": change_pct,
                "change_amount": change_amount,
                "pe": None,  # 新浪实时数据不含PE，PE由财务接口提供
                "turnover_rate": None,
                "total_mv": None,
                "circ_mv": None,
                "source": "sina",
            }
        except Exception as e:
            _logger.debug(f"[sina_client] 解析失败 {sym}: {e}")
            continue

    return results


def _parse_tencent_batch(text: str, codes: list) -> Dict[str, Dict[str, Any]]:
    """
    解析腾讯批量响应
    text 形如：v_sh600519="49~贵州茅台~1800.00~1805.00~1800.00~1809.00~1795.00~1800.00~100~...";
    """
    results = {}
    code_map = {normalize_code(c): c for c in codes}

    for line in text.strip().split("\n"):
        m = re.search(r'v_(\w+)="([^"]*)"', line)
        if not m:
            continue
        sym = m.group(1)  # sh600519
        vals = m.group(2).split("~")
        if len(vals) < 40:
            continue

        raw_code = sym[2:] if sym.startswith(("sh", "sz")) else sym
        code = code_map.get(raw_code, raw_code)

        try:
            current = float(vals[3]) if vals[3] else 0
            prev_close = float(vals[4]) if vals[4] else 0
            open_price = float(vals[5]) if vals[5] else 0
            vol = int(float(vals[6])) if vals[6] else 0  # 手
            high = float(vals[33]) if vals[33] else 0
            low = float(vals[34]) if vals[34] else 0
            amount = float(vals[37]) if vals[37] else 0
            change_pct = 0.0
            if prev_close and prev_close != 0:
                change_pct = round((current - prev_close) / prev_close * 100, 2)
            change_amount = round(current - prev_close, 2) if current else 0
            pe = float(vals[39]) if vals[39] and vals[39] not in ("-", "") else None

            results[code] = {
                "code": code,
                "name": vals[1],
                "price": current,
                "prev_close": prev_close,
                "open": open_price,
                "high": high,
                "low": low,
                "volume": vol * 100,
                "amount": amount,
                "change_pct": change_pct,
                "change_amount": change_amount,
                "pe": pe,
                "source": "tencent",
            }
        except Exception as e:
            _logger.debug(f"[sina_client] 腾讯解析失败 {sym}: {e}")
            continue

    return results


def get_realtime_sina(codes: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    获取实时行情（新浪 API，批量）
    返回 {"code": {...}, ...}，code 为原始6位代码
    """
    if not codes:
        return {}

    # 收集 Sina 格式 → 原始代码 的反向映射
    sina_to_orig = {}
    sina_codes_list = []
    for c in codes:
        c = normalize_code(c)
        sym = to_sina_symbol(c)          # sh600519 / sz002353
        sina_codes_list.append(sym)
        sina_to_orig[sym] = c

    codes_str = ",".join(sina_codes_list)
    headers = {
        "Referer": "https://finance.sina.com.cn",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    }

    try:
        with httpx.Client(timeout=8) as client:
            resp = client.get(SINA_BATCH_URL.format(codes=codes_str), headers=headers)
            resp.raise_for_status()
            text = resp.content.decode("gbk", errors="replace")
            raw = _parse_sina_batch(text, sina_codes_list)  # 内部用 Sina 符号
            # 映射回原始代码
            return {sina_to_orig.get(sym, sym): v for sym, v in raw.items()}
    except Exception as e:
        _logger.debug(f"[sina_client] 新浪请求失败: {e}")

    return {}


def get_realtime_tencent(codes: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    获取实时行情（腾讯 API，批量）
    """
    if not codes:
        return {}

    sina_to_orig = {}
    sina_codes_list = []
    for c in codes:
        c = normalize_code(c)
        sym = to_sina_symbol(c)
        sina_codes_list.append(sym)
        sina_to_orig[sym] = c

    codes_str = ",".join(sina_codes_list)
    headers = {
        "Referer": "https://gu.qq.com",
        "User-Agent": "Mozilla/5.0",
    }

    try:
        with httpx.Client(timeout=8) as client:
            resp = client.get(TENCENT_BATCH_URL.format(codes=codes_str), headers=headers)
            resp.raise_for_status()
            text = resp.content.decode("gbk", errors="replace")
            raw = _parse_tencent_batch(text, sina_codes_list)
            return {sina_to_orig.get(sym, sym): v for sym, v in raw.items()}
    except Exception as e:
        _logger.debug(f"[sina_client] 腾讯请求失败: {e}")

    return {}


def get_realtime(codes: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    获取实时行情（优先级：新浪 → 腾讯 → akshare）
    """
    codes = [normalize_code(c) for c in codes if c]

    # 1. 新浪
    result = get_realtime_sina(codes)
    found = set(result.keys())

    # 2. 腾讯补漏
    missing = [c for c in codes if c not in found]
    if missing:
        extra = get_realtime_tencent(missing)
        result.update(extra)

    return result


# ─────────────────────────────────────────
# 大盘指数
# ─────────────────────────────────────────

SINA_INDEX_URL = "https://hq.sinajs.cn/list={codes}"
INDEX_MAP = {
    "上证指数": "sh000001",
    "深证成指": "sz399001",
    "创业板指": "sz399006",
    "科创50": "sh000688",
}


def get_market_index_sina() -> Dict[str, Dict[str, Any]]:
    """
    获取大盘指数（新浪，批量）
    返回 {指数名: {close, change_pct, ...}}
    """
    # Sina 格式: sh000001 / sz399001 等
    sina_codes = list(INDEX_MAP.values())  # ['sh000001', 'sz399001', ...]
    # 反向映射: Sina符号 → 指数名
    sina_to_name = {v: k for k, v in INDEX_MAP.items()}

    codes_str = ",".join(sina_codes)
    headers = {
        "Referer": "https://finance.sina.com.cn",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    }

    try:
        with httpx.Client(timeout=8) as client:
            resp = client.get(SINA_INDEX_URL.format(codes=codes_str), headers=headers)
            resp.raise_for_status()
            text = resp.content.decode("gbk", errors="replace")
            raw = _parse_sina_batch(text, sina_codes)  # {sina_sym: {...}}
            # 映射到指数名
            result = {}
            for sym, data in raw.items():
                name = sina_to_name.get(sym, sym)
                data["name"] = name
                result[name] = data
            return result
    except Exception as e:
        _logger.debug(f"[sina_client] 新浪指数请求失败: {e}")
        return {}


def get_market_index() -> Dict[str, Dict[str, Any]]:
    """
    获取大盘指数（优先级：新浪 → 腾讯 → akshare）
    """
    result = get_market_index_sina()
    if result:
        return result

    # 腾讯补漏
    tencent = get_realtime_tencent(list(INDEX_MAP.values()))
    return tencent


if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    import json

    print("=== 新浪实时行情验证 ===")
    stocks = ["002353", "002487", "300870"]
    rt = get_realtime(stocks)
    for code, d in rt.items():
        print(f"  {d.get('name', code)}({code}): 现价={d.get('price')} 涨跌={d.get('change_pct')}% PE={d.get('pe')} 成交额={d.get('amount')}")

    print()
    print("=== 大盘指数 ===")
    mi = get_market_index()
    for name in INDEX_MAP.keys():
        d = mi.get(name, {})
        print(f"  {name}: {d.get('price')} ({d.get('change_pct')}%)")
