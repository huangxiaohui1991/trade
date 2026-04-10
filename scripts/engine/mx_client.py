#!/usr/bin/env python3
"""
engine/mx_client.py — mx-skills API 客户端（同步版）

EM_API_KEY 从环境变量读取，调用东方财富 ai-saas 接口。

响应体结构（已验证）：
  data.searchDataResultDTO.dataTableDTOList[i].nameMap   → {"f2": "最新价", "f3": "涨跌幅", ...}
  data.searchDataResultDTO.dataTableDTOList[i].rawTable  → {"f2": ["113.77"], "f3": ["1.73"], ...}
  data.searchDataResultDTO.dataTableDTOList[i].headName  → ["2026-04-09 11:12"]
"""

import os
import re
import uuid
from typing import Optional, Dict, Any, List

import httpx

try:
    from scripts.utils.logger import get_logger
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    def get_logger(name): return logging.getLogger(name)

_logger = get_logger("mx_client")

EM_API_KEY = os.environ.get("EM_API_KEY", "")
MX_SEARCH_URL = "https://ai-saas.eastmoney.com/proxy/b/mcp/tool/searchData"

# 简单内存缓存（同分钟）
_CACHE: Dict[str, tuple] = {}   # key -> (min_ts, data)
_CACHE_TTL = 60


def _now_ts():
    from datetime import datetime
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def _http_post(query: str, timeout: float = 20.0) -> Optional[Dict[str, Any]]:
    """调 mx searchData 接口"""
    if not EM_API_KEY:
        _logger.warning("[mx_client] EM_API_KEY 未设置")
        return None
    try:
        body = {
            "query": query,
            "toolContext": {
                "callId": f"call_{uuid.uuid4().hex[:8]}",
                "userInfo": {"userId": f"user_{uuid.uuid4().hex[:8]}"},
            },
        }
        with httpx.Client(timeout=timeout) as client:
            resp = client.post(
                MX_SEARCH_URL,
                json=body,
                headers={"Content-Type": "application/json", "em_api_key": EM_API_KEY},
            )
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        _logger.debug(f"[mx_client] 请求失败: {e}")
        return None


def _parse_mx_table(data: Dict) -> Dict[str, Any]:
    """
    解析 mx API 返回的 dataTableDTO。

    响应结构（已验证）：
      dto.nameMap   = {"f2": "最新价", "f3": "涨跌幅", "f5": "成交量", "f115": "市盈率(TTM)", ...}
      dto.rawTable  = {"f2": ["113.54"], "f3": ["1.53"], "f5": ["18810828"], "f115": ["40.98"], "headName": ["2026-04-09 11:15"]}

    返回 {"字段名": 值} 字典。
    """
    try:
        dto_list = (
            data.get("data", {})
            .get("searchDataResultDTO", {})
            .get("dataTableDTOList", [])
        )
    except Exception:
        return {}

    for dto in dto_list:
        name_map = dto.get("nameMap", {})    # field_key → 中文名
        raw_table = dto.get("rawTable", {})  # field_key → [值, ...]（首元素=最新）

        result = {}
        for field_key, field_name in name_map.items():
            if field_name in ("数据来源", "headNameSub"):
                continue
            raw_vals = raw_table.get(field_key, [])
            if not isinstance(raw_vals, list):
                raw_vals = [raw_vals]
            if len(raw_vals) == 0:
                continue

            # 取最新值（数组首元素）
            val = raw_vals[0]

            # 清洗
            if isinstance(val, str):
                val = val.strip().replace("%", "").replace(",", "")
                if val == "-" or val == "":
                    val = None
                else:
                    try:
                        val = float(val)
                    except ValueError:
                        pass  # 保留原始字符串

            result[field_name] = val

        if result:
            # 附加上数据时间
            ts_list = raw_table.get("headName", [])
            if ts_list:
                result["_ts"] = ts_list[0]
            return result

    return {}


def _num(v, default=None):
    try:
        if v is None:
            return default
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip().replace("%", "").replace(",", "").replace("万", "").replace("亿", "")
        if s == "-" or s == "":
            return default
        return float(s)
    except Exception:
        return default


def _fmt_amount(v):
    """成交量格式化：万/亿"""
    try:
        if v is None:
            return None
        if isinstance(v, str):
            v = v.strip()
            if v.endswith("万"):
                return float(v[:-1]) * 10000
            elif v.endswith("亿"):
                return float(v[:-1]) * 1e8
            else:
                return float(v.replace(",", ""))
        return float(v)
    except Exception:
        return None


# ─────────────────────────────────────────
# 1. 实时行情
# ─────────────────────────────────────────

def get_realtime_mx(codes: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    通过 mx API 获取多只股票实时行情。
    返回 {"code": {price, change_pct, volume, pe, ...}}
    """
    results = {}

    for code in codes:
        code = str(code).strip()
        if not code:
            continue

        # 判断交易所后缀
        if code[0] in ("6", "5", "9"):
            sym = f"sh{code}"
        else:
            sym = f"sz{code}"

        cache_key = f"rt_{code}"
        now_min = _now_ts()[:16]
        if cache_key in _CACHE:
            ts, data = _CACHE[cache_key]
            if ts == now_min:
                results[code] = data
                continue

        query = (
            f"显示股票{sym}的实时行情：最新价、涨跌幅、涨跌额、成交量、成交额、"
            f"今开、昨收、最高、最低、市盈率-动态、市净率、换手率、总市值、流通市值"
        )
        data = _http_post(query)
        if not data:
            continue

        parsed = _parse_mx_table(data)
        if not parsed:
            continue

        # 字段名可能有多种映射，做模糊匹配
        def get(fields, default=None):
            for f in fields:
                if f in parsed and parsed[f] is not None:
                    return parsed[f]
            return default

        results[code] = {
            "code": code,
            "name": get(["股票名称", "证券名称", "名称"], code),
            "price": _num(get(["最新价", "现价", "当前价"])),
            "change_pct": _num(get(["涨跌幅"])),
            "change_amount": _num(get(["涨跌额", "涨跌"])),
            "volume": _num(get(["成交量"])),
            "amount": _num(get(["成交额"])),
            "open": _num(get(["今开", "开盘价", "今开价"])),
            "prev_close": _num(get(["昨收", "昨收价"])),
            "high": _num(get(["最高", "最高价"])),
            "low": _num(get(["最低", "最低价"])),
            "pe": _num(get(["市盈率-动态", "市盈率(TTM)", "PE"])),
            "pb": _num(get(["市净率", "PB"])),
            "turnover_rate": _num(get(["换手率"])),
            "total_mv": _num(get(["总市值"])),
            "circ_mv": _num(get(["流通市值"])),
            "source": "mx_api",
        }
        _CACHE[cache_key] = (now_min, results[code])

    return results


# ─────────────────────────────────────────
# 2. 大盘指数
# ─────────────────────────────────────────

_INDEX_MAP = {
    "上证指数": "sh000001",
    "深证成指": "sz399001",
    "创业板指": "sz399006",
    "科创50": "sh000688",
}

def get_market_index_mx() -> Dict[str, Dict[str, Any]]:
    """通过 mx API 获取四大指数实时行情

    指数的特殊性：
    - "当前点位"查询返回 f2(最新价) + headName(实时时间戳)
    - "成交量/涨跌幅"查询返回历史数据（无 f2）
    故分两次查询：点位一次，涨跌额/成交量从历史数据取最新
    """
    results = {}

    for name, symbol in _INDEX_MAP.items():
        cache_key = f"idx_{name}"
        now_min = _now_ts()[:16]
        if cache_key in _CACHE:
            ts, data = _CACHE[cache_key]
            if ts == now_min:
                results[name] = data
                continue

        # 第一次：当前点位
        data_price = _http_post(f"{name} 当前点位")
        price_val = None
        if data_price:
            p = _parse_mx_table(data_price)
            price_val = _num(p.get("最新价"))

        # 第二次：涨跌幅/成交量（历史数据）
        chg_pct = None
        chg_amount = None
        volume = None
        open_price = None
        high = None
        low = None

        data_ohlcv = _http_post(f"{name} 涨跌幅 涨跌额 成交量 开盘价 最高价 最低价")
        if data_ohlcv:
            dto_list = (
                data_ohlcv.get("data", {})
                .get("searchDataResultDTO", {})
                .get("dataTableDTOList", [])
            )
            for dto in dto_list:
                name_map = dto.get("nameMap", {})
                raw_table = dto.get("rawTable", {})
                # 历史数据：第一行是最新的
                for fk, fn in name_map.items():
                    if fn in ("数据来源", "headNameSub"):
                        continue
                    vals = raw_table.get(fk, [])
                    if isinstance(vals, list) and len(vals) > 0:
                        v = vals[0]
                        v = _num(v) if isinstance(v, str) else (_num(v) if v is not None else None)
                        if fn == "涨跌幅":
                            chg_pct = v
                        elif fn == "涨跌额":
                            chg_amount = v
                        elif fn == "成交量":
                            volume = v
                        elif fn in ("开盘价", "今开"):
                            open_price = v
                        elif fn in ("最高价", "最高"):
                            high = v
                        elif fn in ("最低价", "最低"):
                            low = v

        results[name] = {
            "close": price_val,
            "change_pct": chg_pct,
            "change_amount": chg_amount,
            "open": open_price,
            "high": high,
            "low": low,
            "volume": volume,
            "source": "mx_api",
        }
        _CACHE[cache_key] = (now_min, results[name])

    return results


# ─────────────────────────────────────────
# 3. 历史日线（用于技术指标）
# ─────────────────────────────────────────

def get_hist_data_mx(code: str, days: int = 60) -> Optional[List[Dict]]:
    """通过 mx API 获取历史日线 OHLCV"""
    query = (
        f"显示股票{code}最近{days}个交易日的日期、开盘价、收盘价、最高价、最低价、"
        f"成交量、成交额、涨跌幅"
    )
    data = _http_post(query)
    if not data:
        return None

    try:
        dto_list = (
            data.get("data", {})
            .get("searchDataResultDTO", {})
            .get("dataTableDTOList", [])
        )
        for dto in dto_list:
            table = dto.get("rawTable", {})
            if not table:
                continue

            # 找日期列
            date_key = None
            for k in table.keys():
                if "日期" in str(k):
                    date_key = k
                    break
            if not date_key:
                continue

            dates = table.get(date_key, [])
            if not isinstance(dates, list):
                continue

            result = []
            for i, d in enumerate(dates):
                row = {"date": str(d)}
                for field_key, vals in table.items():
                    if field_key == date_key or field_key == "headName":
                        continue
                    if isinstance(vals, list) and i < len(vals):
                        v = vals[i]
                        row[field_key] = _num(v) if v != "-" else None
                    else:
                        row[field_key] = None
                result.append(row)
            return result
    except Exception as e:
        _logger.debug(f"[mx_client] 解析 {code} 历史数据失败: {e}")

    return None


# ─────────────────────────────────────────
# 4. 财务数据
# ─────────────────────────────────────────

def get_financial_mx(code: str) -> Optional[Dict[str, Any]]:
    """通过 mx API 获取财务数据"""
    query = (
        f"显示股票{code}的最新财务数据：营业收入、净利润、净资产收益率ROE、"
        f"经营性现金流、市净率、市盈率PE、每股收益、每股净资产"
    )
    data = _http_post(query)
    if not data:
        return None
    return _parse_mx_table(data)


if __name__ == "__main__":
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    print("=== mx_client 验证 ===")

    print("\n[1] 实时行情: get_realtime_mx(['002353', '002487', '300870'])")
    rt = get_realtime_mx(["002353", "002487", "300870"])
    for code, d in rt.items():
        print(f"  {code}: price={d.get('price')} pe={d.get('pe')} chg={d.get('change_pct')}%")

    print("\n[2] 大盘指数: get_market_index_mx()")
    mi = get_market_index_mx()
    for name, d in mi.items():
        print(f"  {name}: close={d.get('close')} chg={d.get('change_pct')}%")

    print("\n=== 验证完成 ===")
