#!/usr/bin/env python3
"""
engine/financial.py — 基本面数据模块

职责：
  - get_financial: ROE / 营收增长 / 现金流

数据源（按优先级）：
  1. 妙想 mx_data API（优先）
  2. 东方财富 akshare（fallback，重试 3 次）
  3. 新浪财经（最终 fallback）
"""

import os
import sys
import time
import warnings
from datetime import datetime

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

os.environ["TQDM_DISABLE"] = "1"
warnings.filterwarnings("ignore")

import pandas as pd
import akshare as ak
from scripts.utils.cache import load_json_cache, save_json_cache

try:
    from scripts.utils.logger import get_logger
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    def get_logger(name):
        return logging.getLogger(name)

_logger = get_logger("financial")


def _now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def _sina_symbol(code: str) -> str:
    """转换代码为新浪格式"""
    code = str(code).zfill(6)
    if code.startswith(("6", "9")):
        return "sh" + code
    else:
        return "sz" + code


# ── 新浪财务数据（东财被封时的 fallback）───────────────────────────────────

def _sina_financial(code: str) -> dict:
    """通过新浪接口获取财务数据"""
    sym = _sina_symbol(code)

    result = {}
    try:
        # 利润表
        df_pl = ak.stock_financial_report_sina(stock=sym, symbol="利润表")
        if df_pl is not None and not df_pl.empty:
            # 找营业收入
            rev_col = next((c for c in df_pl.columns if "营业总收入" in c or "营业收入" in c), None)
            profit_col = next((c for c in df_pl.columns if "净利润" in c and "归属" in c), None)
            if rev_col:
                result["revenues"] = df_pl[rev_col].dropna().head(8).tolist()
            if profit_col:
                result["profits"] = df_pl[profit_col].dropna().head(8).tolist()
    except Exception as e:
        _logger.warning(f"[_sina_financial] 利润表失败: {e}")

    try:
        # 现金流量表
        df_cf = ak.stock_financial_report_sina(stock=sym, symbol="现金流量表")
        if df_cf is not None and not df_cf.empty:
            cash_col = next((c for c in df_cf.columns if "经营活动产生的现金流量净额" in c or ("经营活动" in c and "净额" in c)), None)
            if cash_col:
                result["cash_flows"] = df_cf[cash_col].dropna().head(4).tolist()
    except Exception as e:
        _logger.warning(f"[_sina_financial] 现金流量表失败: {e}")

    return result


# ── 东财指标接口（报表接口挂时的 fallback）──────────────────────────────

def _fill_from_indicator(code: str, result: dict, missing_fields: list) -> None:
    """
    用东财 stock_financial_analysis_indicator 补充缺失字段。
    这个接口和报表接口是不同的后端，报表挂了这个可能还活着。
    """
    try:
        df = ak.stock_financial_analysis_indicator(symbol=code, start_year="2024")
        if df is None or df.empty:
            return
    except Exception:
        return

    if "revenue" in missing_fields and "revenue_growth" not in result:
        rev_col = next((c for c in df.columns if "主营业务收入增长率" in c), None)
        if rev_col:
            vals = df[rev_col].dropna().head(4).tolist()
            if vals:
                result["revenue_growth"] = round(float(vals[0]), 2)
                result["source_chain"].append("eastmoney_indicator_revenue")
                missing_fields[:] = [f for f in missing_fields if f != "revenue"]
                _logger.info(f"[get_financial] 东财指标补充营收增长: {result['revenue_growth']}%")

    if "cash_flow" in missing_fields and "operating_cash_flow" not in result:
        cf_col = next((c for c in df.columns if "每股经营性现金流" in c), None)
        if cf_col:
            vals = df[cf_col].dropna().head(4).tolist()
            if vals:
                cf_per_share = float(vals[0])
                result["cash_flow_per_share"] = cf_per_share
                result["cash_flow_positive"] = cf_per_share > 0
                result["source_chain"].append("eastmoney_indicator_cashflow")
                missing_fields[:] = [f for f in missing_fields if f != "cash_flow"]
                _logger.info(f"[get_financial] 东财指标补充现金流: {cf_per_share} 元/股")

    if "roe" in missing_fields and "roe" not in result:
        roe_col = next((c for c in df.columns if "净资产收益率" in c and "加权" not in c), None)
        if roe_col:
            vals = df[roe_col].dropna().head(4).tolist()
            if vals:
                result["roe"] = round(float(vals[0]), 2)
                result["roe_recent"] = [round(float(v), 2) for v in vals]
                result["source_chain"].append("eastmoney_indicator_roe")
                missing_fields[:] = [f for f in missing_fields if f != "roe"]


# ── 腾讯实时行情（行情数据 fallback）──────────────────────────────────────

def _tencent_realtime(code: str) -> dict | None:
    """
    通过腾讯 web.sqt.gtimg.cn 获取实时行情。
    返回 {price, change_pct, name, pe, pb, turnover_rate, total_mv, circ_mv}
    """
    import urllib.request
    prefix = "sh" if code.startswith(("6", "9")) else "sz"
    url = f"https://web.sqt.gtimg.cn/q={prefix}{code}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            content = resp.read().decode("gbk", errors="ignore")
        parts = content.split("~")
        if len(parts) < 50:
            return None
        price = float(parts[3]) if parts[3] else 0
        if price <= 0:
            return None
        return {
            "name": parts[1],
            "price": price,
            "change_pct": float(parts[32]) if parts[32] else 0,
            "open": float(parts[5]) if parts[5] else 0,
            "high": float(parts[33]) if parts[33] else 0,
            "low": float(parts[34]) if parts[34] else 0,
            "prev_close": float(parts[4]) if parts[4] else 0,
            "volume": int(float(parts[36]) * 100) if parts[36] else 0,
            "amount": float(parts[37]) * 10000 if parts[37] else 0,
            "turnover_rate": float(parts[38]) if parts[38] else 0,
            "pe": float(parts[39]) if parts[39] else 0,
            "total_mv": float(parts[45]) * 1e8 if parts[45] else 0,
            "circ_mv": float(parts[44]) * 1e8 if parts[44] else 0,
            "pb": float(parts[46]) if len(parts) > 46 and parts[46] else 0,
            "source": "tencent_realtime",
        }
    except Exception as e:
        _logger.info(f"[tencent] {code} 实时行情失败: {e}")
        return None


# ── 腾讯日线历史（日线数据 fallback）──────────────────────────────────────

def _tencent_hist(code: str, days: int = 60) -> "pd.DataFrame | None":
    """
    通过 akshare stock_zh_a_hist_tx 获取腾讯日线数据。
    返回 DataFrame(date, open, close, high, low, amount) 或 None。
    """
    prefix = "sh" if code.startswith(("6", "9")) else "sz"
    symbol = f"{prefix}{code}"
    try:
        from datetime import timedelta
        end = datetime.now().strftime("%Y%m%d")
        start = (datetime.now() - timedelta(days=days + 30)).strftime("%Y%m%d")
        df = ak.stock_zh_a_hist_tx(symbol=symbol, start_date=start, end_date=end)
        if df is not None and not df.empty:
            return df.tail(days)
        return None
    except Exception as e:
        _logger.info(f"[tencent] {code} 日线历史失败: {e}")
        return None


# ── 主接口 ─────────────────────────────────────────────────────────────────

def _try_mx_financial(code: str, name: str) -> dict:
    """
    通过妙想 mx_data API 获取基本面数据（优先数据源）。
    成功返回标准 dict，失败返回 None。
    """
    try:
        from scripts.mx.mx_data import MXData
        mx = MXData()

        # 查询 ROE + 营收 + 现金流
        query = f"{name} 最近一年ROE 营收增长率 经营现金流"
        result = mx.query(query)
        tables, _, _, err = MXData.parse_result(result)

        if err or not tables:
            _logger.info(f"[mx_financial] {name} 查询无结果: {err}")
            return None

        # 解析返回的表格数据
        parsed = {"source": "mx_data"}
        for table in tables:
            for row in table.get("rows", []):
                for key, val in row.items():
                    key_lower = key.lower() if isinstance(key, str) else ""
                    val_str = str(val).strip().replace("%", "").replace(",", "")

                    try:
                        num = float(val_str) if val_str and val_str not in ("", "-", "—") else None
                    except (ValueError, TypeError):
                        num = None

                    if num is None:
                        continue

                    if "roe" in key_lower or "净资产收益率" in key:
                        parsed.setdefault("roe", round(num, 2))
                    elif "营收" in key and ("增长" in key or "同比" in key):
                        parsed.setdefault("revenue_growth", round(num, 2))
                    elif "营业" in key and ("收入" in key or "总收入" in key):
                        parsed.setdefault("_revenue", num)
                    elif "经营" in key and "现金" in key:
                        parsed.setdefault("operating_cash_flow", num)
                        parsed.setdefault("cash_flow_positive", num > 0)

        if "roe" in parsed or "revenue_growth" in parsed:
            _logger.info(f"[mx_financial] {name} MX 成功: roe={parsed.get('roe')} rev={parsed.get('revenue_growth')}")
            return parsed

        return None
    except Exception as e:
        _logger.info(f"[mx_financial] {name} MX 异常: {e}")
        return None


def get_financial(code: str) -> dict:
    """
    获取基本面数据（东财优先带重试，失败则新浪 fallback）

    Returns:
        {
            "code": str,
            "name": str,
            "roe": float (%),
            "roe_recent": [float],
            "revenue_growth": float (最近一季同比%),
            "revenue_growth_detail": [str],
            "operating_cash_flow": float,
            "cash_flow_positive": bool,
            "source": str,
            "error": str,
        }
    """
    from scripts.engine.technical import normalize_code, get_stock_name

    code = normalize_code(code)
    result = {
        "code": code,
        "timestamp": _now_ts(),
        "stale": False,
        "source": None,
        "source_chain": [],
        "cached_at": None,
    }

    try:
        result["name"] = get_stock_name(code)
    except Exception:
        result["name"] = code

    missing_fields = []  # 哪些字段东财没有拿到

    # ── MX 优先：妙想 API 获取基本面 ────────────────────────────────────────
    mx_data = _try_mx_financial(code, result.get("name", code))
    if mx_data:
        result["source_chain"].append("mx_data")
        if "roe" in mx_data:
            result["roe"] = mx_data["roe"]
            result["roe_recent"] = [mx_data["roe"]]
        if "revenue_growth" in mx_data:
            result["revenue_growth"] = mx_data["revenue_growth"]
        if "operating_cash_flow" in mx_data:
            result["operating_cash_flow"] = mx_data["operating_cash_flow"]
            result["cash_flow_positive"] = mx_data.get("cash_flow_positive", mx_data["operating_cash_flow"] > 0)
        result["source"] = "mx_data"

        # 检查是否所有字段都有了
        has_roe = "roe" in result
        has_rev = "revenue_growth" in result
        has_cf = "operating_cash_flow" in result
        if has_roe and has_rev and has_cf:
            save_json_cache("financial", code, result, meta={"source": "mx_data"})
            return result
        # 部分字段缺失，继续用 akshare 补充
        if not has_roe:
            missing_fields.append("roe")
        if not has_rev:
            missing_fields.append("revenue")
        if not has_cf:
            missing_fields.append("cash_flow")
        _logger.info(f"[get_financial] MX 部分缺失 {missing_fields}，akshare 补充")

    # ── ROE（东财）───────────────────────────────────────────────────────────
    if "roe" not in result:
      for attempt in range(3):
        try:
            df = ak.stock_financial_analysis_indicator(symbol=code, start_year="2024")
            if df is not None and not df.empty:
                roe_col = next((c for c in df.columns if "净资产收益率" in str(c)), None)
                if roe_col:
                    roe_values = df[roe_col].dropna().head(4).tolist()
                    result["roe_recent"] = roe_values
                    result["roe"] = round(float(roe_values[0]), 2)
                    result["source_chain"].append("eastmoney_roe")
                    break
        except Exception as e:
            if attempt < 2:
                time.sleep(2)
                _logger.info(f"[get_financial] ROE东财重试 ({attempt+2}/3) code={code}")
            else:
                _logger.warning(f"[get_financial] ROE获取失败 code={code}: {e}")
                missing_fields.append("roe")

    # ── 营收增长（东财报表，重试失败则记为缺失）─────────────────────────────
    if "revenue_growth" not in result:
      for attempt in range(3):
        try:
            df_profit = ak.stock_profit_sheet_by_report_em(symbol=code)
            if df_profit is not None and not df_profit.empty:
                rev_col = next((c for c in df_profit.columns if "营业总收入" in c or "营业收入" in c), None)
                if rev_col:
                    revenues = df_profit[rev_col].dropna().head(4).tolist()
                    result["revenue_recent_quarters"] = revenues
                    if len(revenues) >= 2 and revenues[1] and revenues[1] != 0:
                        growth = (revenues[0] - revenues[1]) / abs(revenues[1])
                        result["revenue_growth"] = round(growth * 100, 2)
                        result["revenue_growth_detail"] = [
                            f"{(revenues[i] - revenues[i+1]) / abs(revenues[i+1]) * 100:.1f}%"
                            if i + 1 < len(revenues) and revenues[i+1] else "N/A"
                            for i in range(min(2, len(revenues) - 1))
                        ]
                    result["source_chain"].append("eastmoney_revenue")
                    break
            break
        except Exception as e:
            if attempt < 2:
                time.sleep(2)
                _logger.info(f"[get_financial] 营收东财重试 ({attempt+2}/3) code={code}")
            else:
                _logger.warning(f"[get_financial] 营收获取失败 code={code}: {e}")
    if "revenue_growth" not in result:
        missing_fields.append("revenue")

    # ── 现金流（东财报表，重试失败则记为缺失）─────────────────────────────
    if "operating_cash_flow" not in result:
      for attempt in range(3):
        try:
            df_cash = ak.stock_cash_flow_sheet_by_report_em(symbol=code)
            if df_cash is not None and not df_cash.empty:
                cash_col = next((c for c in df_cash.columns if "经营活动" in c and "现金流" in c), None)
                if cash_col:
                    result["operating_cash_flow"] = float(df_cash[cash_col].iloc[0])
                    result["cash_flow_positive"] = result["operating_cash_flow"] > 0
                    result["source_chain"].append("eastmoney_cashflow")
                    break
            break
        except Exception as e:
            if attempt < 2:
                time.sleep(2)
                _logger.info(f"[get_financial] 现金流东财重试 ({attempt+2}/3) code={code}")
            else:
                _logger.warning(f"[get_financial] 现金流获取失败 code={code}: {e}")
    if "operating_cash_flow" not in result:
        missing_fields.append("cash_flow")

    # ── 东财指标接口 fallback（报表接口挂时用指标接口补）──────────────────
    if missing_fields:
        try:
            _fill_from_indicator(code, result, missing_fields)
        except Exception as e:
            _logger.info(f"[get_financial] 东财指标接口 fallback 失败 code={code}: {e}")

    # ── 新浪 fallback：补充仍然缺失的字段 ────────────────────────────────
    if missing_fields:
        _logger.warning(f"[get_financial] 东财缺失字段 {missing_fields}，尝试新浪 fallback code={code}")
        try:
            sina = _sina_financial(code)
            if sina:
                if "revenue" in missing_fields and "revenues" in sina and sina["revenues"]:
                    result["revenue_recent_quarters"] = sina["revenues"]
                    revs = sina["revenues"]
                    if len(revs) >= 2 and revs[1] and revs[1] != 0:
                        growth = (revs[0] - revs[1]) / abs(revs[1])
                        result["revenue_growth"] = round(growth * 100, 2)
                        result["revenue_growth_detail"] = [
                            f"{(revs[i] - revs[i+1]) / abs(revs[i+1]) * 100:.1f}%"
                            if i + 1 < len(revs) and revs[i+1] else "N/A"
                            for i in range(min(2, len(revs) - 1))
                        ]
                if "cash_flow" in missing_fields and "cash_flows" in sina and sina["cash_flows"]:
                    result["operating_cash_flow"] = float(sina["cash_flows"][0])
                    result["cash_flow_positive"] = result["operating_cash_flow"] > 0
                if "roe" in missing_fields and "profits" in sina:
                    revs = sina.get("revenues", [])
                    profits = sina["profits"]
                    if revs and profits and revs[0] and profits[0] and revs[0] > 0:
                        result["roe_estimate"] = round(profits[0] / revs[0] * 100, 2)
                result["source_chain"].append("sina_financial")
        except Exception as e:
            _logger.warning(f"[get_financial] 新浪 fallback 失败 code={code}: {e}")

    result["source"] = "eastmoney" + ("+sina" if missing_fields else "")
    if any(k in result for k in ["roe", "revenue_growth", "operating_cash_flow"]):
        save_json_cache("financial", code, result, meta={"source": result["source"]})
    else:
        cached = load_json_cache("financial", code, max_age_seconds=86400)
        if cached:
            cached_data = cached.get("data", {})
            if isinstance(cached_data, dict):
                cached_data["stale"] = True
                cached_data["cached_at"] = cached.get("cached_at")
                chain = list(cached_data.get("source_chain", []))
                chain.append("cache_financial")
                cached_data["source_chain"] = chain
                return cached_data
    return result


# ---------------------------------------------------------------------------
# 验证
# ------------------------------------------------------------------------

if __name__ == "__main__":
    for code in ["002487", "002353", "300870"]:
        r = get_financial(code)
        print(f"\n{code} ({r.get('name')}):")
        print(f"  source={r.get('source')}  roe={r.get('roe')}  rev_growth={r.get('revenue_growth')}%")
        print(f"  现金流={r.get('operating_cash_flow')}  正={r.get('cash_flow_positive')}")
        if "error" in r:
            print(f"  ERROR: {r['error']}")
