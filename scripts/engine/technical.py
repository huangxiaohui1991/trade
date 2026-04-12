#!/usr/bin/env python3
"""
engine/technical.py — 技术指标模块

职责：
  - get_technical: 技术面数据（均线/成交量/当前价vs均线）
  - normalize_code / to_sina_symbol / get_stock_name: 工具函数（供全 engine 层共享）

数据源（按优先级）：
  - get_stock_name: MX优先 → 东财 fallback
  - _get_hist_data: 东财(akshare) → 新浪 fallback
  - 注：历史日线需要完整 DataFrame 计算均线，MX 不适合批量拉日线
"""

import os
import sys
import time
import warnings
from datetime import datetime, timedelta
from typing import Optional

from scripts.utils.exceptions import DataSourceError

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

os.environ["TQDM_DISABLE"] = "1"
warnings.filterwarnings("ignore")

import pandas as pd
import akshare as ak

try:
    from scripts.utils.logger import get_logger
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    def get_logger(name):
        return logging.getLogger(name)

_logger = get_logger("technical")


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------

_name_cache = {}


def normalize_code(code: str) -> str:
    """标准化股票代码（去掉前缀 sh/sz/bj）"""
    code = code.strip()
    for prefix in ["sh", "sz", "SH", "SZ", "bj", "BJ"]:
        if code.startswith(prefix):
            code = code[len(prefix):]
    return code


def to_sina_symbol(code: str) -> str:
    """转换为新浪格式 sh600001 / sz000001"""
    code = normalize_code(code)
    if code.startswith("6") or code.startswith("9"):
        return f"sh{code}"
    return f"sz{code}"


def get_stock_name(code: str) -> str:
    """获取股票名称（带缓存，MX优先 → 东财 fallback）"""
    code = normalize_code(code)
    if code in _name_cache:
        return _name_cache[code]

    # MX 优先：从妙想查询股票名称（TTL 24h，几乎不变）
    try:
        from scripts.mx.mx_data import _cached_mx_query, TTL_NAME
        result = _cached_mx_query(f"{code}最新价", TTL_NAME)
        dto_list = result.get("data", {}).get("data", {}).get("searchDataResultDTO", {}).get("dataTableDTOList", [])
        if dto_list:
            tag = dto_list[0].get("entityTagDTO", {})
            name = tag.get("fullName", "")
            if name:
                _name_cache[code] = name
                return name
    except Exception:
        pass

    # 东财 fallback
    try:
        df = ak.stock_individual_info_em(symbol=code)
        name_row = df[df["item"] == "股票简称"]
        if not name_row.empty:
            name = str(name_row.iloc[0]["value"])
            _name_cache[code] = name
            return name
    except Exception as e:
        _logger.warning(f"[get_stock_name] 东财接口失败 code={code}: {e}")

    _name_cache[code] = code
    return code


def _now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def _get_hist_data(code: str, days: int = 120) -> pd.DataFrame:
    """
    获取历史日线数据（MX优先 → 东财 → 新浪）
    """
    code = normalize_code(code)
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=days + 30)).strftime("%Y%m%d")

    # Source 0: 妙想 mx_data API（优先，TTL 6h）
    try:
        from scripts.mx.mx_data import _cached_mx_query, TTL_HIST
        name = _name_cache.get(code, code)
        query = f"{name if name != code else code}近{days + 60}个交易日每天的收盘价和成交量"
        result = _cached_mx_query(query, TTL_HIST)
        data = result.get("data", {}).get("data", {}).get("searchDataResultDTO", {})
        dto_list = data.get("dataTableDTOList", [])

        # 找到有历史数据的表（headName 长度 > 10）
        for dto in dto_list:
            table = dto.get("table", {})
            name_map = dto.get("nameMap", {})
            heads = table.get("headName", [])
            if not isinstance(heads, list) or len(heads) < 10:
                continue

            data_keys = [k for k in table.keys() if k != "headName"]
            if not data_keys:
                continue

            # 识别收盘价列和成交量列
            close_key = None
            vol_key = None
            for k in data_keys:
                mapped = str(name_map.get(k, name_map.get(str(k), "")))
                if "收盘" in mapped or "最新" in mapped:
                    close_key = k
                elif "成交量" in mapped:
                    vol_key = k

            if not close_key:
                continue

            close_vals = table.get(close_key, [])
            vol_vals = table.get(vol_key, []) if vol_key else []

            rows = []
            for idx, date_str in enumerate(heads):
                date_clean = str(date_str).split("(")[0].strip()
                close_str = str(close_vals[idx]).replace("元", "").replace(",", "").strip() if idx < len(close_vals) else "0"
                vol_str = str(vol_vals[idx]).replace("股", "").replace(",", "").strip() if idx < len(vol_vals) else "0"
                # 处理 "万" "亿" 单位
                try:
                    close_v = float(close_str)
                except (ValueError, TypeError):
                    close_v = 0
                try:
                    vol_v = float(vol_str)
                    if "亿" in str(vol_vals[idx] if idx < len(vol_vals) else ""):
                        vol_v *= 1e8
                    elif "万" in str(vol_vals[idx] if idx < len(vol_vals) else ""):
                        vol_v *= 1e4
                except (ValueError, TypeError):
                    vol_v = 0

                if close_v > 0:
                    rows.append({"日期": date_clean, "收盘": close_v, "成交量": vol_v})

            if len(rows) < 10:
                continue

            df = pd.DataFrame(rows)
            # MX 返回倒序，翻转为正序
            df = df.iloc[::-1].reset_index(drop=True)
            df["涨跌幅"] = df["收盘"].pct_change() * 100
            df["开盘"] = df["收盘"]
            df["最高"] = df["收盘"]
            df["最低"] = df["收盘"]
            _logger.info(f"[_get_hist_data] MX 成功 code={code} rows={len(df)}")
            return df

    except Exception as e:
        _logger.info(f"[_get_hist_data] MX 失败 code={code}: {e}")

    # Source 1: 东方财富（带重试）
    for attempt in range(2):
        try:
            df = ak.stock_zh_a_hist(
                symbol=code, period="daily",
                start_date=start_date, end_date=end_date,
                adjust="qfq"
            )
            if df is not None and not df.empty:
                return df
            break
        except Exception as e:
            if attempt < 1:
                time.sleep(1)
                _logger.info(f"[_get_hist_data] 东财重试 ({attempt+2}/2) code={code}")
            else:
                _logger.warning(f"[_get_hist_data] 东财失败 code={code}: {e}")

    # Source 2: 新浪
    try:
        time.sleep(0.3)
        df = ak.stock_zh_a_daily(symbol=to_sina_symbol(code), adjust="qfq")
        if df is not None and not df.empty:
            df = df.rename(columns={
                "date": "日期", "open": "开盘", "close": "收盘",
                "high": "最高", "low": "最低", "volume": "成交量",
            })
            df["涨跌幅"] = df["收盘"].pct_change() * 100
            df["日期"] = pd.to_datetime(df["日期"])
            start_dt = pd.to_datetime(start_date)
            df = df[df["日期"] >= start_dt].copy()
            df["日期"] = df["日期"].dt.strftime("%Y-%m-%d")
            return df
    except Exception as e:
        _logger.warning(f"[_get_hist_data] 新浪失败 code={code}: {e}")

    # Source 3: 腾讯
    try:
        from scripts.engine.financial import _tencent_hist
        df = _tencent_hist(code, days)
        if df is not None and not df.empty:
            df = df.rename(columns={
                "date": "日期", "open": "开盘", "close": "收盘",
                "high": "最高", "low": "最低", "amount": "成交量",
            })
            if "涨跌幅" not in df.columns:
                df["涨跌幅"] = df["收盘"].pct_change() * 100
            _logger.info(f"[_get_hist_data] 腾讯成功 code={code} rows={len(df)}")
            return df
    except Exception as e:
        _logger.warning(f"[_get_hist_data] 腾讯失败 code={code}: {e}")

    return pd.DataFrame()


# ---------------------------------------------------------------------------
# 技术指标
# ---------------------------------------------------------------------------

def get_technical(code: str, days: int = 60) -> dict:
    """
    获取技术面数据

    Returns:
        {
            "code": str,
            "name": str,
            "current_price": float,
            "change_pct": float,
            "ma": {"MA5": float, "MA10": float, "MA20": float, "MA60": float},
            "above_ma20": bool,
            "above_ma60": bool,
            "ma60_direction": str,  # "向上" / "向下" / "走平"
            "volume_analysis": {
                "today": int,
                "MA5": int,
                "MA20": int,
                "score": float,
                "breakout_1_5x": bool,
            },
            "volume_ratio": float,
            "momentum_5d": float,
            "hist": DataFrame,
            "error": str (if failed),
            "stale": bool,
        }
    """
    code = normalize_code(code)
    result = {
        "code": code,
        "timestamp": _now_ts(),
        "stale": False,
    }

    try:
        df = _get_hist_data(code, days + 60)

        if df is None or df.empty:
            result["error"] = "无法获取行情数据"
            result["stale"] = True
            return result

        # 计算均线
        df = df.copy()
        df["MA5"] = df["收盘"].rolling(5).mean()
        df["MA10"] = df["收盘"].rolling(10).mean()
        df["MA20"] = df["收盘"].rolling(20).mean()
        df["MA60"] = df["收盘"].rolling(60).mean()
        df["VOL_MA5"] = df["成交量"].rolling(5).mean()
        df["VOL_MA20"] = df["成交量"].rolling(20).mean()

        latest = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else latest

        current_price = float(latest["收盘"])
        ma5 = float(latest["MA5"]) if pd.notna(latest["MA5"]) else None
        ma10 = float(latest["MA10"]) if pd.notna(latest["MA10"]) else None
        ma20 = float(latest["MA20"]) if pd.notna(latest["MA20"]) else None
        ma60 = float(latest["MA60"]) if pd.notna(latest["MA60"]) else None

        # MA60 方向
        ma60_5d_ago = float(df.iloc[-6]["MA60"]) if len(df) > 5 and pd.notna(df.iloc[-6]["MA60"]) else None
        if ma60 and ma60_5d_ago:
            if ma60 > ma60_5d_ago * 1.001:
                ma60_direction = "向上"
            elif ma60 < ma60_5d_ago * 0.999:
                ma60_direction = "向下"
            else:
                ma60_direction = "走平"
        else:
            ma60_direction = None

        # 成交量分析
        vol_today = float(latest["成交量"])
        vol_ma5 = float(latest["VOL_MA5"]) if pd.notna(latest["VOL_MA5"]) else None
        vol_ma20 = float(latest["VOL_MA20"]) if pd.notna(latest["VOL_MA20"]) else None

        volume_score = 0
        if vol_ma20 and vol_today > vol_ma20:
            volume_score = 1.0
        elif vol_ma20:
            recent_3d = df.tail(3)
            if any(recent_3d["成交量"] > vol_ma20):
                volume_score = 0.5

        volume_breakout = bool(vol_ma5 and vol_today > vol_ma5 * 1.5)

        # 量比
        vol_ratio = round(vol_today / vol_ma5, 2) if vol_ma5 else 1.0

        # 近5日动量（涨幅%）
        recent_5d = df.tail(5)
        if len(recent_5d) >= 2:
            first_close = float(recent_5d.iloc[0]["收盘"])
            last_close = float(recent_5d.iloc[-1]["收盘"])
            momentum_5d = ((last_close / first_close) - 1) * 100 if first_close else 0
        else:
            momentum_5d = 0

        result.update({
            "name": get_stock_name(code),
            "date": str(latest["日期"]),
            "current_price": round(current_price, 2),
            "change_pct": round(float(latest["涨跌幅"]), 2),
            "volume": int(vol_today),
            "ma": {
                "MA5": round(ma5, 2) if ma5 else None,
                "MA10": round(ma10, 2) if ma10 else None,
                "MA20": round(ma20, 2) if ma20 else None,
                "MA60": round(ma60, 2) if ma60 else None,
            },
            "above_ma20": current_price > ma20 if ma20 else None,
            "above_ma60": current_price > ma60 if ma60 else None,
            "ma60_direction": ma60_direction,
            "volume_analysis": {
                "today": int(vol_today),
                "MA5": int(vol_ma5) if vol_ma5 else None,
                "MA20": int(vol_ma20) if vol_ma20 else None,
                "above_ma20": vol_today > vol_ma20 if vol_ma20 else None,
                "score": volume_score,
                "breakout_1_5x": volume_breakout,
            },
            "volume_ratio": vol_ratio,
            "momentum_5d": round(momentum_5d, 2),
            "hist": df,  # 保留原始 DataFrame 供其他模块使用
        })
        return result

    except DataSourceError as e:
        _logger.warning(f"[get_technical] 技术面获取失败 code={code}: {e}")
        result["error"] = str(e)
        result["stale"] = True
        return result


# ---------------------------------------------------------------------------
# 验证入口
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json

    test_codes = ["002487", "002353", "300870"]
    print(f"=== technical.py 验证 ===")
    for code in test_codes:
        r = get_technical(code, 60)
        name_str = r.get("name", "?")
        print(f"\n{code} ({name_str}):")
        print(f"  price={r.get('current_price')}  above_ma20={r.get('above_ma20')}")
        print(f"  MA20={r.get('ma',{}).get('MA20')}  MA60={r.get('ma',{}).get('MA60')}")
        print(f"  ma60_direction={r.get('ma60_direction')}")
        print(f"  vol_ratio={r.get('volume_ratio')}  vol_score={r.get('volume_analysis',{}).get('score')}")
        print(f"  momentum_5d={r.get('momentum_5d')}")
        if "error" in r:
            print(f"  ERROR: {r['error']}")
