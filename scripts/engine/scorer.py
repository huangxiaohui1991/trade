#!/usr/bin/env python3
"""
engine/scorer.py — 四维评分引擎

职责：
  - 四维评分：技术面(2分) + 基本面(3分) + 资金(2分) + 舆情(3分)
  - apply_veto: 一票否决规则检查
  - batch_score: 批量评分

设计原则：
  - 各维度独立计算，失败返回 error 不阻塞总分
  - veto 触发直接返回 0 分
"""

import os
import sys
import warnings
from typing import Optional

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

os.environ["TQDM_DISABLE"] = "1"
warnings.filterwarnings("ignore")

from scripts.engine.technical import get_technical, normalize_code
from scripts.engine.financial import get_financial
from scripts.engine.flow import get_fund_flow
from scripts.utils.config_loader import get_strategy
from scripts.utils.logger import get_logger

_logger = get_logger("scorer")


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------

def _score_technical(code: str, tech_data: dict) -> dict:
    """
    技术面评分（满分 2 分）
    均线(0.5) + 成交量(0.5) + 动量(0.5) + 均线排列(0.5)
    """
    if "error" in tech_data:
        return {"score": 0, "detail": f"数据错误: {tech_data['error']}"}

    price = tech_data.get("current_price", 0)
    ma = tech_data.get("ma", {})
    ma20 = ma.get("MA20", 0)
    ma60 = ma.get("MA60", 0)
    ma5 = ma.get("MA5", 0)
    above_ma20 = tech_data.get("above_ma20", False)

    # 均线得分 (0.5)
    if above_ma20 and price >= ma60:
        ma_score = 0.5
    elif above_ma20:
        ma_score = 0.3
    else:
        ma_score = 0

    # 成交量得分 (0.5)
    vol_score = tech_data.get("volume_analysis", {}).get("score", 0)

    # 动量得分 (0.5)
    momentum = tech_data.get("momentum_5d", 0)
    if momentum >= 5:
        mom_score = 0.5
    elif momentum >= 2:
        mom_score = 0.3
    elif momentum >= 0:
        mom_score = 0.1
    else:
        mom_score = 0

    # 均线排列 (0.5)
    arr_score = 0
    if ma5 and ma20 and ma60:
        if ma5 > ma20 > ma60:
            arr_score = 0.5
        elif ma20 > ma60:
            arr_score = 0.3

    total = round(min(ma_score + vol_score + mom_score + arr_score, 2.0), 1)
    return {
        "score": total,
        "detail": f"均线:{ma_score}/0.5 量:{vol_score}/0.5 动量:{mom_score}/0.5 排列:{arr_score}/0.5",
    }


def _score_fundamental(code: str, fin_data: dict) -> dict:
    """
    基本面评分（满分 3 分）
    ROE(1) + 营收增长(1) + 现金流(1)
    """
    if "error" in fin_data:
        return {"score": 0, "detail": f"数据错误: {fin_data.get('error', fin_data)}"}

    # ROE (0-1 分)
    roe = fin_data.get("roe", 0)
    if roe is None:
        roe = 0
    if roe >= 15:
        roe_score = 1.0
    elif roe >= 10:
        roe_score = 0.7
    elif roe >= 5:
        roe_score = 0.4
    else:
        roe_score = 0

    # 营收增长 (0-1 分)
    rev_growth = fin_data.get("revenue_growth", 0)
    if rev_growth is None:
        rev_growth = 0
    if rev_growth >= 20:
        rev_score = 1.0
    elif rev_growth >= 10:
        rev_score = 0.7
    elif rev_growth >= 0:
        rev_score = 0.3
    else:
        rev_score = 0

    # 现金流 (0-1 分)
    cf_score = 0.5 if fin_data.get("cash_flow_positive", False) else 0

    total = round(min(roe_score + rev_score + cf_score, 3.0), 1)
    return {
        "score": total,
        "detail": f"ROE:{roe_score:.1f}/1 营收:{rev_score:.1f}/1 现金流:{cf_score:.1f}/1",
        "roe": roe,
        "revenue_growth": rev_growth,
    }


def _score_fund_flow(code: str, flow_data: dict) -> dict:
    """
    资金流评分（满分 2 分）
    主力净流入(1) + 北向(1)
    """
    if "error" in flow_data:
        return {"score": 0, "detail": f"数据错误: {flow_data.get('error', '')}"}

    # 主力净流入 (0-1 分)
    main_net = flow_data.get("main_net_inflow", 0)
    if main_net > 1_000_000_000:
        main_score = 1.0
    elif main_net > 500_000_000:
        main_score = 0.7
    elif main_net > 0:
        main_score = 0.4
    else:
        main_score = 0

    # 北向资金 (0-1 分)
    north = flow_data.get("northbound", {})
    net_5d = north.get("net_5d_positive")
    if net_5d is True:
        north_score = 1.0
    elif net_5d is None:
        north_score = 0.5  # 数据不可用
    else:
        north_score = 0

    total = round(min(main_score + north_score, 2.0), 1)
    return {
        "score": total,
        "detail": f"主力:{main_score}/1.0 北向:{north_score}/1.0",
        "main_net_inflow": main_net,
        "main_outflow": flow_data.get("main_outflow", False),
    }


def _score_sentiment(code: str, name: str) -> dict:
    """
    舆情评分（满分 3 分）
    TrendRadar 接入（目前返回默认 1.5 分中性）
    TODO: 接入 TrendRadar API
    """
    return {
        "score": 1.5,
        "detail": "TrendRadar 待接入（默认1.5分）",
        "sentiment": "neutral",
    }


def apply_veto(tech_data: dict, flow_data: dict, strategy: dict) -> list:
    """
    检查一票否决规则

    Returns:
        触发的一票否决信号列表，空列表=通过
    """
    veto_signals = []
    veto_rules = strategy.get("scoring", {}).get("veto", [])

    if "below_ma20" in veto_rules and not tech_data.get("above_ma20", True):
        veto_signals.append("below_ma20")

    if "limit_up_today" in veto_rules:
        change_pct = abs(tech_data.get("change_pct", 0))
        if change_pct >= 9.9:
            veto_signals.append("limit_up_today")

    if "consecutive_outflow" in veto_rules:
        if flow_data.get("major_outflow_streak", 0) >= 3:
            veto_signals.append("consecutive_outflow")

    if "red_market" in veto_rules:
        from scripts.engine.market_timer import get_signal
        if get_signal() == "RED":
            veto_signals.append("red_market")

    return veto_signals


def score(code: str, name: Optional[str] = None) -> dict:
    """
    对单个股票进行四维评分

    Returns:
        {
            "name": str,
            "code": str,
            "technical_score": float,
            "fundamental_score": float,
            "flow_score": float,
            "sentiment_score": float,
            "total_score": float,
            "veto_signals": list,
            "veto_triggered": bool,
            "weights": dict,
        }
    """
    code = normalize_code(code)
    strategy = get_strategy()
    weights = strategy.get("scoring", {}).get("weights", {})

    tech = get_technical(code, 60)
    fin = get_financial(code)
    flow = get_fund_flow(code, 5)
    sentiment = _score_sentiment(code, name or tech.get("name", ""))

    tech_s = _score_technical(code, tech)
    fin_s = _score_fundamental(code, fin)
    flow_s = _score_fund_flow(code, flow)

    # 一票否决
    veto_signals = apply_veto(tech, flow, strategy)
    veto_triggered = len(veto_signals) > 0

    # 加权总分（满分 10）
    raw = (
        tech_s["score"] * weights.get("technical", 2) / 2 +
        fin_s["score"] * weights.get("fundamental", 3) / 3 +
        flow_s["score"] * weights.get("flow", 2) / 2 +
        sentiment["score"] * weights.get("sentiment", 3) / 3
    )

    if veto_triggered:
        total = 0.0
    else:
        total = round(raw, 1)

    return {
        "name": name or tech.get("name", code),
        "code": code,
        "technical_score": tech_s["score"],
        "fundamental_score": fin_s["score"],
        "flow_score": flow_s["score"],
        "sentiment_score": sentiment["score"],
        "total_score": total,
        "technical_detail": tech_s["detail"],
        "fundamental_detail": fin_s["detail"],
        "flow_detail": flow_s["detail"],
        "sentiment_detail": sentiment["detail"],
        "veto_signals": veto_signals,
        "veto_triggered": veto_triggered,
        "weights": weights,
        # 原始数据
        "_raw": {"technical": tech, "fundamental": fin, "fund_flow": flow},
    }


def batch_score(stocks: list) -> list:
    """
    批量评分

    Args:
        stocks: list of {"code": str, "name": str}

    Returns:
        评分结果列表（按总分降序）
    """
    results = []
    for item in stocks:
        code = normalize_code(item.get("code", ""))
        name = item.get("name")
        try:
            r = score(code, name)
            results.append(r)
            _logger.info(
                f"[scorer] {r['name']}({code}): "
                f"技术{r['technical_score']:.1f} 基本面{r['fundamental_score']:.1f} "
                f"资金{r['flow_score']:.1f} 舆情{r['sentiment_score']:.1f} "
                f"→ 总分{r['total_score']:.1f}"
                + (f" ❌ veto:{','.join(r['veto_signals'])}" if r['veto_triggered'] else "")
            )
        except Exception as e:
            _logger.error(f"[scorer] 评分失败 {code}: {e}")
            results.append({
                "code": code,
                "name": name or code,
                "total_score": 0,
                "veto_triggered": True,
                "veto_signals": ["score_error"],
                "error": str(e),
            })

    results.sort(key=lambda x: x.get("total_score", 0), reverse=True)
    return results


# ---------------------------------------------------------------------------
# 验证
# ------------------------------------------------------------------------

if __name__ == "__main__":
    test_stocks = [
        {"code": "002487", "name": "大金重工"},
        {"code": "002353", "name": "杰瑞股份"},
        {"code": "300870", "name": "欧陆通"},
    ]
    print("=== scorer.py 验证 ===")
    results = batch_score(test_stocks)
    print()
    for r in results:
        veto = f" ❌ veto:{','.join(r['veto_signals'])}" if r.get('veto_triggered') else " ✅"
        print(f"{r['name']}({r['code']}): 总分={r['total_score']:.1f}{veto}")
        print(f"  技术={r['technical_score']:.1f} 基本面={r['fundamental_score']:.1f} "
              f"资金={r['flow_score']:.1f} 舆情={r['sentiment_score']:.1f}")
