"""
market/mx/realtime.py — 东财 ai-saas 实时行情客户端

从 V1 scripts/engine/mx_client.py 迁移。
使用 httpx 同步客户端（adapter 层用 to_thread 包装为 async）。
"""

from __future__ import annotations

import logging
import os
import uuid
from typing import Any, Dict, List, Optional

import httpx

_logger = logging.getLogger(__name__)

EM_API_KEY = os.environ.get("EM_API_KEY", "")
MX_SEARCH_URL = "https://ai-saas.eastmoney.com/proxy/b/mcp/tool/searchData"

_CACHE: Dict[str, tuple] = {}


def _now_min() -> str:
    from datetime import datetime
    return datetime.now().strftime("%Y-%m-%dT%H:%M")


def _http_post(query: str, timeout: float = 20.0) -> Optional[Dict[str, Any]]:
    if not EM_API_KEY:
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
            resp = client.post(MX_SEARCH_URL, json=body,
                               headers={"Content-Type": "application/json", "em_api_key": EM_API_KEY})
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        _logger.debug(f"mx request failed: {e}")
        return None


def _num(v, default=None):
    try:
        if v is None:
            return default
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip().replace("%", "").replace(",", "").replace("万", "").replace("亿", "")
        return float(s) if s and s != "-" else default
    except Exception:
        return default


def _parse_mx_table(data: Dict) -> Dict[str, Any]:
    try:
        dto_list = data.get("data", {}).get("searchDataResultDTO", {}).get("dataTableDTOList", [])
    except Exception:
        return {}
    for dto in dto_list:
        name_map = dto.get("nameMap", {})
        raw_table = dto.get("rawTable", {})
        result = {}
        for fk, fn in name_map.items():
            if fn in ("数据来源", "headNameSub"):
                continue
            vals = raw_table.get(fk, [])
            if not isinstance(vals, list):
                vals = [vals]
            if not vals:
                continue
            val = vals[0]
            if isinstance(val, str):
                val = val.strip().replace("%", "").replace(",", "")
                if val in ("-", ""):
                    val = None
                else:
                    try:
                        val = float(val)
                    except ValueError:
                        pass
            result[fn] = val
        if result:
            ts_list = raw_table.get("headName", [])
            if ts_list:
                result["_ts"] = ts_list[0]
            return result
    return {}


def get_realtime_mx(codes: List[str]) -> Dict[str, Dict[str, Any]]:
    """批量获取实时行情。"""
    results = {}
    for code in codes:
        code = str(code).strip()
        if not code:
            continue
        sym = f"sh{code}" if code[0] in ("6", "5", "9") else f"sz{code}"
        cache_key = f"rt_{code}"
        now = _now_min()
        if cache_key in _CACHE and _CACHE[cache_key][0] == now:
            results[code] = _CACHE[cache_key][1]
            continue
        data = _http_post(f"显示股票{sym}的实时行情：最新价、涨跌幅、成交量、成交额、今开、最高、最低、市盈率-动态、市净率、换手率")
        if not data:
            continue
        p = _parse_mx_table(data)
        if not p:
            continue

        def get(fields, default=None):
            for f in fields:
                if f in p and p[f] is not None:
                    return p[f]
            return default

        entry = {
            "code": code, "name": get(["股票名称", "证券名称", "名称"], code),
            "price": _num(get(["最新价", "现价"])), "change_pct": _num(get(["涨跌幅"])),
            "volume": _num(get(["成交量"])), "amount": _num(get(["成交额"])),
            "open": _num(get(["今开", "开盘价"])), "high": _num(get(["最高", "最高价"])),
            "low": _num(get(["最低", "最低价"])), "pe": _num(get(["市盈率-动态", "市盈率(TTM)"])),
            "pb": _num(get(["市净率"])), "turnover_rate": _num(get(["换手率"])),
            "source": "mx_api",
        }
        results[code] = entry
        _CACHE[cache_key] = (now, entry)
    return results


_INDEX_MAP = {"上证指数": "sh000001", "深证成指": "sz399001", "创业板指": "sz399006", "科创50": "sh000688"}


def get_market_index_mx() -> Dict[str, Dict[str, Any]]:
    """获取四大指数实时行情。"""
    results = {}
    for name, symbol in _INDEX_MAP.items():
        cache_key = f"idx_{name}"
        now = _now_min()
        if cache_key in _CACHE and _CACHE[cache_key][0] == now:
            results[name] = _CACHE[cache_key][1]
            continue
        data = _http_post(f"{name} 当前点位")
        price = None
        if data:
            price = _num(_parse_mx_table(data).get("最新价"))
        data2 = _http_post(f"{name} 涨跌幅 涨跌额 成交量")
        chg_pct = None
        if data2:
            for dto in data2.get("data", {}).get("searchDataResultDTO", {}).get("dataTableDTOList", []):
                nm = dto.get("nameMap", {})
                rt = dto.get("rawTable", {})
                for fk, fn in nm.items():
                    vals = rt.get(fk, [])
                    if isinstance(vals, list) and vals and fn == "涨跌幅":
                        chg_pct = _num(vals[0])
        entry = {"close": price, "change_pct": chg_pct, "source": "mx_api"}
        results[name] = entry
        _CACHE[cache_key] = (now, entry)
    return results
