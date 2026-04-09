#!/usr/bin/env python3
"""
engine/flow.py — 资金流向模块

职责：
  - get_fund_flow: 主力资金流向

数据源（按优先级）：
  1. 妙想 mx_data API（优先）
  2. 东方财富个股资金流向（stock_individual_fund_flow）— 带 3 次重试
  3. 东财主力资金流向（stock_main_fund_flow）— 备用
  4. 历史成交量（腾讯）— 最终 fallback
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

_logger = get_logger("flow")


def _now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def _get_northbound(days: int = 10) -> dict:
    """获取北向资金流向（沪股通+深股通合计）"""
    result = {"timestamp": _now_ts(), "stale": False}

    try:
        df_sh = ak.stock_hsgt_hist_em(symbol="沪股通")
        df_sz = ak.stock_hsgt_hist_em(symbol="深股通")
        if df_sh is None or df_sz is None:
            result["error"] = "无法获取北向资金"
            result["stale"] = True
            return result

        df_sh = df_sh.tail(days)
        df_sz = df_sz.tail(days)

        flows = []
        for i in range(len(df_sh)):
            sh_row = df_sh.iloc[i]
            sz_row = df_sz.iloc[i] if i < len(df_sz) else None
            sh_net = float(sh_row.get("当日成交净买额", 0)) if pd.notna(sh_row.get("当日成交净买额")) else 0
            sz_net = float(sz_row.get("当日成交净买额", 0)) if sz_row is not None and pd.notna(sz_row.get("当日成交净买额")) else 0
            flows.append({
                "date": str(sh_row.get("日期", "")),
                "net_flow": round((sh_net + sz_net) / 1e8, 2),  # 亿元
            })

        last_5 = flows[-5:] if len(flows) >= 5 else flows
        net_5d = sum(f["net_flow"] for f in last_5)
        all_zero = all(f["net_flow"] == 0 for f in last_5)

        result.update({
            "recent_flows": flows,
            "net_5d": round(net_5d, 2),
            "net_5d_positive": net_5d > 0 if not all_zero else None,
            "data_available": not all_zero,
            "unit": "亿元",
        })
        return result
    except Exception as e:
        _logger.warning(f"[_get_northbound] 北向资金获取失败: {e}")
        result["error"] = str(e)
        result["stale"] = True
        return result


def _parse_em_fund_flow(df: pd.DataFrame, days: int) -> dict:
    """解析东方财富资金流向 DataFrame，返回标准字段"""
    recent = df.tail(days)
    total_net = 0
    major_outflow_streak = 0

    for _, row in recent.iterrows():
        main_net = None
        for col in row.index:
            col_str = str(col)
            if "主力" in col_str and "净" in col_str:
                main_net = float(row[col]) if pd.notna(row[col]) else 0
                break
        if main_net is None:
            for col in row.index:
                if "主力" in str(col):
                    main_net = float(row[col]) if pd.notna(row[col]) else 0
                    break
        if main_net is not None:
            total_net += main_net
            if main_net < -5_000_000:
                major_outflow_streak += 1

    return {
        "main_net_inflow": total_net,
        "main_outflow": total_net < 0,
        "no_major_outflow": major_outflow_streak < 3,
        "major_outflow_streak": major_outflow_streak,
    }


def get_fund_flow(code: str, days: int = 5) -> dict:
    """
    获取个股主力资金流向（三源 fallback，带重试）

    Returns:
        {
            "code": str,
            "source": str,
            "main_net_inflow": float (元),
            "main_outflow": bool,
            "no_major_outflow": bool,
            "major_outflow_streak": int,
            "northbound": dict (北向数据),
            "error": str,
        }
    """
    from scripts.engine.technical import normalize_code, _get_hist_data

    code = normalize_code(code)
    result = {
        "code": code,
        "timestamp": _now_ts(),
        "stale": False,
        "source": None,
        "sources_tried": [],
        "cached_at": None,
    }

    # ── Source 0: 妙想 mx_data API（优先）────────────────────────────────────
    result["sources_tried"].append("mx_data")
    try:
        from scripts.mx.mx_data import MXData
        from scripts.engine.technical import get_stock_name
        name = get_stock_name(code)
        mx = MXData()
        mx_result = mx.query(f"{name} 主力资金流向 近5日")
        tables, _, _, err = MXData.parse_result(mx_result)

        if not err and tables:
            # 解析主力净流入
            main_net = 0
            outflow_days = 0
            for table in tables:
                for row in table.get("rows", []):
                    for key, val in row.items():
                        val_str = str(val).strip().replace(",", "")
                        try:
                            num = float(val_str) if val_str and val_str not in ("", "-", "—") else None
                        except (ValueError, TypeError):
                            num = None
                        if num is None:
                            continue
                        if "主力" in key and ("净" in key or "流入" in key):
                            main_net += num
                            if num < 0:
                                outflow_days += 1

            if main_net != 0 or outflow_days > 0:
                result.update({
                    "source": "mx_data",
                    "main_net_inflow": main_net,
                    "main_outflow": main_net < 0,
                    "no_major_outflow": outflow_days < 3,
                    "major_outflow_streak": outflow_days,
                })
                result["northbound"] = _get_northbound(days)
                save_json_cache("flow", code, result, meta={"source": "mx_data"})
                _logger.info(f"[get_fund_flow] MX 成功 code={code} net={main_net/1e6:.1f}M")
                return result
    except Exception as e:
        _logger.info(f"[get_fund_flow] MX 失败 code={code}: {e}")

    # ── Source 1: 东财个股资金流向（带 3 次重试）────────────────────────────
    result["sources_tried"].append("eastmoney_individual")
    for attempt in range(3):
        try:
            market = "sh" if code.startswith(("6", "9")) else "sz"
            df = ak.stock_individual_fund_flow(stock=code, market=market)
            if df is not None and not df.empty:
                parsed = _parse_em_fund_flow(df, days)
                result.update(parsed)
                result["source"] = "eastmoney"
                result["northbound"] = _get_northbound(days)
                save_json_cache("flow", code, result, meta={"source": "eastmoney"})
                return result
            break  # empty df，不重试
        except Exception as e:
            if attempt < 2:
                time.sleep(2)
                _logger.info(f"[get_fund_flow] 东财重试 ({attempt+2}/3) code={code}")
            else:
                _logger.warning(f"[get_fund_flow] 东财失败 code={code}: {e}")

    # ── Source 2: 新浪历史成交量（估算资金流向）─────────────────────────────
    result["sources_tried"].append("sina_hist")
    try:
        df = _get_hist_data(code, days + 5)
        if df is not None and not df.empty and len(df) >= days:
            recent = df.tail(days)
            # 用量比和涨跌幅估算资金净流入方向（无精确数据时的定性判断）
            # 上涨+放量 → 资金流入；下跌+放量 → 资金流出
            score = 0
            for _, row in recent.iterrows():
                chg = float(row.get("涨跌幅", 0))
                vol_ratio = float(row.get("成交量", 0)) / max(float(df["成交量"].mean()), 1)
                if chg > 0 and vol_ratio > 1.2:
                    score += 1  # 流入信号
                elif chg < 0 and vol_ratio > 1.2:
                    score -= 1  # 流出信号
            # score > 0 → 偏流入；score < 0 → 偏流出
            result.update({
                "source": "sina_hist",
                "main_net_inflow": score * 2_000_000,  # 估算值（定性，非精确）
                "main_outflow": score < 0,
                "no_major_outflow": score >= 0,
                "major_outflow_streak": 0,
                "note": f"新浪成交量估算，score={score}/{days}",
            })
            result["northbound"] = _get_northbound(days)
            save_json_cache("flow", code, result, meta={"source": "sina_hist"})
            return result
        else:
            _logger.warning(f"[get_fund_flow] 新浪历史数据为空 code={code}")
    except Exception as e:
        _logger.warning(f"[get_fund_flow] 新浪历史数据备用失败 code={code}: {e}")

    result["error"] = "所有数据源均失败"
    result["stale"] = True
    cached = load_json_cache("flow", code, max_age_seconds=86400)
    if cached:
        cached_data = cached.get("data", {})
        if isinstance(cached_data, dict):
            cached_data["stale"] = True
            cached_data["cached_at"] = cached.get("cached_at")
            cached_data["sources_tried"] = list(cached_data.get("sources_tried", [])) + ["cache_flow"]
            return cached_data
    return result


# ---------------------------------------------------------------------------
# 验证
# ------------------------------------------------------------------------

if __name__ == "__main__":
    test_codes = ["002487", "002353", "300870"]
    print("=== flow.py 验证 ===")
    for code in test_codes:
        r = get_fund_flow(code, 5)
        nb = r.get("northbound", {})
        print(f"\n{code}:")
        print(f"  source={r.get('source')}  main_net={r.get('main_net_inflow', 0)/1e6:.1f}M")
        print(f"  outflow={r.get('main_outflow')}  no_major_outflow={r.get('no_major_outflow')}")
        print(f"  northbound_5d={nb.get('net_5d')}亿元")
        if "error" in r:
            print(f"  ERROR: {r['error']}")
