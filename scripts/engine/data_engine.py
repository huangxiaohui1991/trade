#!/usr/bin/env python3
"""
engine/data_engine.py — 数据获取引擎（精简版 + Facade）

职责：
  - get_realtime: 实时行情（多股批量，MX优先 → 东财实时接口）
  - get_market_index: 大盘指数状态
  - normalize_code / to_sina_symbol / get_stock_name: 工具函数

其他模块（已拆分）：
  - technical.py: get_technical（均线/成交量/动量）
  - financial.py: get_financial（ROE/营收/现金流）— MX 优先
  - flow.py: get_fund_flow（主力/北向资金）— MX 优先
  - scorer.py: batch_score（四维评分）— 舆情用 MX 搜索
  - risk_model.py: calc_stop_loss / check_risk（风控）

设计原则：
  - 每个数据源独立 try/except，失败写 logger.warning()
  - 全失败才返回 error 标记，不抛异常
  - 返回数据统一带 timestamp / stale 标记
"""

import os
import sys
import warnings
from datetime import datetime

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

os.environ["TQDM_DISABLE"] = "1"
warnings.filterwarnings("ignore")

import akshare as ak
import pandas as pd

try:
    from scripts.utils.logger import get_logger
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    def get_logger(name):
        return logging.getLogger(name)

_logger = get_logger("data_engine")

# 重新导出工具函数（供其他模块便捷访问）
from scripts.engine.technical import (
    normalize_code,
    to_sina_symbol,
    get_stock_name,
    _now_ts,
)
from scripts.engine.technical import _get_hist_data


# ---------------------------------------------------------------------------
# 实时行情
# ---------------------------------------------------------------------------

def get_realtime(codes: list) -> dict:
    """
    获取实时行情（批量）

    Args:
        codes: 股票代码列表，如 ["600000", "000001"]

    Returns:
        {
            "timestamp": "2026-04-09T09:30:00",
            "stale": False,
            "data": {
                "600000": { "code": "600000", "name": "...", "price": 10.5, ... },
                ...
            },
            "errors": { "600036": "东财接口失败" }
        }
    """
    from scripts.engine.technical import normalize_code, _get_hist_data, get_stock_name

    result = {
        "timestamp": _now_ts(),
        "stale": False,
        "data": {},
        "errors": {}
    }

    if not codes:
        return result

    # 东财实时行情接口（批量）
    try:
        df = ak.stock_zh_a_spot_em()
        for code in codes:
            code = normalize_code(code)
            row = df[df["代码"] == code]
            if not row.empty:
                r = row.iloc[0]
                result["data"][code] = {
                    "code": code,
                    "name": str(r.get("名称", "")),
                    "price": float(r.get("最新价", 0)),
                    "change_pct": float(r.get("涨跌幅", 0)),
                    "change_amount": float(r.get("涨跌额", 0)),
                    "volume": int(r.get("成交量", 0)),
                    "amount": float(r.get("成交额", 0)),
                    "high": float(r.get("最高", 0)),
                    "low": float(r.get("最低", 0)),
                    "open": float(r.get("今开", 0)),
                    "prev_close": float(r.get("昨收", 0)),
                    "turnover_rate": float(r.get("换手率", 0)),
                    "pe": float(r.get("市盈率-动态", 0)) if pd.notna(r.get("市盈率-动态")) else None,
                    "pb": float(r.get("市净率", 0)) if pd.notna(r.get("市净率")) else None,
                    "total_mv": float(r.get("总市值", 0)),
                    "circ_mv": float(r.get("流通市值", 0)),
                    "source": "realtime",
                }
            else:
                result["errors"][code] = "代码不在实时行情列表中"
    except Exception as e:
        _logger.warning(f"[get_realtime] 东财实时接口失败: {e}")
        result["stale"] = True

        # fallback 1: 妙想 mx_data API
        try:
            from scripts.mx.mx_data import MXData
            mx = MXData()
            for code in codes:
                code = normalize_code(code)
                if code in result["data"]:
                    continue
                try:
                    name = get_stock_name(code)
                    mx_result = mx.query(f"{name} 最新价 涨跌幅")
                    tables, _, _, err = MXData.parse_result(mx_result)
                    if not err and tables:
                        for row in tables[0].get("rows", []):
                            price_val = None
                            chg_val = 0
                            for k, v in row.items():
                                v_str = str(v).strip().replace(",", "").replace("%", "")
                                try:
                                    num = float(v_str) if v_str and v_str not in ("", "-", "—") else None
                                except (ValueError, TypeError):
                                    num = None
                                if num is None:
                                    continue
                                if "最新" in k or "收盘" in k or "价" in k:
                                    price_val = num
                                elif "涨跌幅" in k:
                                    chg_val = num
                            if price_val:
                                result["data"][code] = {
                                    "code": code,
                                    "name": name,
                                    "price": price_val,
                                    "change_pct": chg_val,
                                    "source": "mx_data",
                                }
                                break
                except Exception:
                    pass
        except Exception as e2:
            _logger.warning(f"[get_realtime] MX fallback 失败: {e2}")

        # fallback 2: 历史日线
        for code in codes:
            code = normalize_code(code)
            try:
                df = _get_hist_data(code, 10)
                if df is not None and not df.empty:
                    r = df.iloc[-1]
                    result["data"][code] = {
                        "code": code,
                        "name": get_stock_name(code),
                        "price": float(r["收盘"]),
                        "change_pct": float(r["涨跌幅"]),
                        "volume": int(r["成交量"]),
                        "amount": float(r.get("成交额", 0)),
                        "high": float(r["最高"]),
                        "low": float(r["最低"]),
                        "open": float(r["开盘"]),
                        "date": str(r["日期"]),
                        "source": "hist_fallback",
                    }
                else:
                    result["errors"][code] = "无法获取行情数据"
            except Exception as e2:
                _logger.warning(f"[get_realtime] fallback失败 code={code}: {e2}")
                result["errors"][code] = str(e2)

    return result


# ---------------------------------------------------------------------------
# 大盘指数
# ---------------------------------------------------------------------------

def get_market_index() -> dict:
    """
    获取大盘状态（上证/创业板）
    - 盘中用东财 Sina 实时接口
    - 盘后用历史收盘
    - MA20/MA60 从历史数据计算
    """
    from datetime import time as dt_time
    from scripts.engine.technical import _get_hist_data

    result = {
        "timestamp": _now_ts(),
        "stale": False,
    }

    indices = {
        "上证指数": "sh000001",
        "深证成指": "sz399001",
        "创业板指": "sz399006",
        "科创50": "sh000688",
    }

    now = datetime.now()
    current_time = now.time()
    is_market_hours = (
        now.weekday() < 5 and (
            dt_time(9, 30) <= current_time <= dt_time(11, 30) or
            dt_time(13, 0) <= current_time <= dt_time(15, 30)
        )
    )

    # 盘中实时数据
    spot_data = {}
    try:
        spot_df = ak.stock_zh_index_spot_sina()
        spot_df = spot_df.set_index("代码")
        for name, symbol in indices.items():
            if symbol in spot_df.index:
                row = spot_df.loc[symbol]
                spot_data[symbol] = {
                    "last": float(row["最新价"]),
                    "prev_close": float(row["昨收"]),
                    "open": float(row["今开"]),
                    "high": float(row["最高"]),
                    "low": float(row["最低"]),
                    "change_pct": float(row["涨跌幅"]),
                    "volume": int(row["成交量"]),
                }
    except Exception as e:
        _logger.warning(f"[get_market_index] 东财实时指数失败: {e}")
        spot_data = {}

    for name, symbol in indices.items():
        try:
            df = ak.stock_zh_index_daily(symbol=symbol)
            if df is None or df.empty:
                result[name] = {"error": "无法获取数据"}
                continue

            df = df.sort_values("date").tail(80).copy()
            df["MA20"] = df["close"].rolling(20).mean()
            df["MA60"] = df["close"].rolling(60).mean()

            latest = df.iloc[-1]
            ma20 = float(latest["MA20"]) if pd.notna(latest["MA20"]) else None
            ma60 = float(latest["MA60"]) if pd.notna(latest["MA60"]) else None

            if symbol in spot_data:
                sd = spot_data[symbol]
                close_price = sd["last"]
                change_pct = sd["change_pct"]
                is_realtime = is_market_hours
            else:
                close_price = float(latest["close"])
                change_pct = None
                is_realtime = False
                result["stale"] = True

            # MA60 下方天数
            below_ma60_days = 0
            if ma60:
                for i in range(len(df) - 1, -1, -1):
                    row_df = df.iloc[i]
                    if pd.notna(row_df["MA60"]) and float(row_df["close"]) < float(row_df["MA60"]):
                        below_ma60_days += 1
                    else:
                        break

            result[name] = {
                "date": now.strftime("%Y-%m-%d") if symbol in spot_data else str(latest["date"]),
                "close": round(close_price, 2),
                "MA20": round(ma20, 2) if ma20 else None,
                "MA60": round(ma60, 2) if ma60 else None,
                "above_MA20": close_price > ma20 if ma20 else None,
                "above_MA60": close_price > ma60 if ma60 else None,
                "below_MA60_days": below_ma60_days,
                "change_pct": round(change_pct, 2) if change_pct is not None else None,
                "realtime": is_realtime,
            }
        except Exception as e:
            _logger.warning(f"[get_market_index] 获取{name}失败: {e}")
            result[name] = {"error": str(e)}
            result["stale"] = True

    # 综合判断
    sh = result.get("上证指数", {})
    cy = result.get("创业板指", {})
    market_ok = sh.get("above_MA20", False) or cy.get("above_MA20", False)
    market_danger = (sh.get("below_MA60_days", 0) >= 3 or cy.get("below_MA60_days", 0) >= 3)

    result["_summary"] = {
        "can_buy": market_ok,
        "should_clear": market_danger,
        "status": "CLEAR" if market_danger else ("BUY" if market_ok else "WARY"),
    }

    return result


# ---------------------------------------------------------------------------
# 便利类（向后兼容）
# ------------------------------------------------------------------------

class DataEngine:
    """Facade 类 — 提供 get_realtime / get_market_index 方法"""

    def get_realtime(self, codes: list):
        return get_realtime(codes)

    def get_market_index(self):
        return get_market_index()

    # 以下方法已移至独立模块，此处保留是为兼容旧代码
    def get_technical(self, code: str, days: int = 60):
        from scripts.engine.technical import get_technical
        return get_technical(code, days)

    def get_financial(self, code: str):
        from scripts.engine.financial import get_financial
        return get_financial(code)

    def get_fund_flow(self, code: str, days: int = 5):
        from scripts.engine.flow import get_fund_flow
        return get_fund_flow(code, days)

    def batch_score(self, codes: list):
        from scripts.engine.scorer import batch_score
        return batch_score([{"code": c} for c in codes])


# ---------------------------------------------------------------------------
# 验证入口
# ------------------------------------------------------------------------

if __name__ == "__main__":
    import json

    print("=== data_engine.py 验证 ===", flush=True)

    engine = DataEngine()

    print("\n[1] get_realtime(['000001', '002487'])", flush=True)
    rt = engine.get_realtime(["000001", "002487"])
    print(json.dumps(rt, ensure_ascii=False, indent=2, default=str), flush=True)

    print("\n[2] get_market_index()", flush=True)
    mi = engine.get_market_index()
    print(json.dumps(mi, ensure_ascii=False, indent=2, default=str), flush=True)

    print("\n=== 验证完成 ===", flush=True)
