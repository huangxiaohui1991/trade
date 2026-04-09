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
    优先使用妙想资讯搜索获取研报/新闻，fallback 返回中性分数。
    """
    try:
        from scripts.mx.mx_search import MXSearch
        mx = MXSearch()
        result = mx.search(f"{name} 最新研报 机构评级")

        data = result.get("data", {})
        inner_data = data.get("data", {})
        search_response = inner_data.get("llmSearchResponse", {})
        items = search_response.get("data", [])

        if not items:
            return {"score": 1.5, "detail": "无相关资讯（MX）", "sentiment": "neutral"}

        # 统计研报数量、评级分布
        report_count = 0
        positive_count = 0
        negative_count = 0
        for item in items:
            info_type = item.get("informationType", "")
            rating = str(item.get("rating", "")).lower()
            if info_type == "REPORT":
                report_count += 1
            if any(w in rating for w in ["买入", "增持", "推荐", "强烈推荐"]):
                positive_count += 1
            elif any(w in rating for w in ["减持", "卖出", "回避"]):
                negative_count += 1

        # 计算舆情分数（0-3分）
        score = 1.5  # 基础分
        if report_count >= 5:
            score += 0.5
        elif report_count >= 2:
            score += 0.3
        if positive_count >= 2:
            score += 0.5
        elif positive_count >= 1:
            score += 0.3
        if negative_count >= 2:
            score -= 0.5

        score = max(0, min(score, 3.0))
        sentiment = "positive" if score >= 2.0 else ("negative" if score < 1.0 else "neutral")

        return {
            "score": round(score, 1),
            "detail": f"研报{report_count}篇 买入{positive_count} 减持{negative_count}（MX搜索）",
            "sentiment": sentiment,
        }
    except Exception as e:
        _logger.info(f"[sentiment] MX搜索失败 {name}: {e}")
        return {
            "score": 1.5,
            "detail": "MX搜索失败，默认1.5分",
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


def _parse_mx_float(row: dict, *keys) -> float:
    """从妙想返回的行数据中提取浮点数，支持模糊列名匹配"""
    for k, v in row.items():
        for key in keys:
            if key in k:
                v_str = str(v).strip().replace(",", "").replace("%", "").replace("亿", "").replace("万", "")
                if "|" in v_str:
                    v_str = v_str.split("|")[0]
                try:
                    num = float(v_str)
                    if "亿" in str(v):
                        num *= 1e8
                    elif "万" in str(v):
                        num *= 1e4
                    return num
                except (ValueError, TypeError):
                    continue
    return 0.0


def _get_real_ma_arrangement(code: str, name: str) -> dict:
    """
    补调 MX 日线，计算真实均线排列和 5 日动量。
    返回 {"ma5": float, "ma20": float, "ma60": float, "momentum_5d": float}
    失败返回空 dict。
    """
    try:
        from scripts.engine.technical import _get_hist_data
        import pandas as pd
        df = _get_hist_data(code, 120)
        if df is None or df.empty or len(df) < 20:
            return {}
        df = df.copy()
        df["MA5"] = df["收盘"].rolling(5).mean()
        df["MA20"] = df["收盘"].rolling(20).mean()
        df["MA60"] = df["收盘"].rolling(60).mean()
        latest = df.iloc[-1]
        ma5 = float(latest["MA5"]) if pd.notna(latest["MA5"]) else None
        ma20 = float(latest["MA20"]) if pd.notna(latest["MA20"]) else None
        ma60 = float(latest["MA60"]) if pd.notna(latest["MA60"]) else None
        # 5日动量
        recent_5d = df.tail(5)
        if len(recent_5d) >= 2:
            first_c = float(recent_5d.iloc[0]["收盘"])
            last_c = float(recent_5d.iloc[-1]["收盘"])
            momentum_5d = ((last_c / first_c) - 1) * 100 if first_c else 0
        else:
            momentum_5d = 0
        return {"ma5": ma5, "ma20": ma20, "ma60": ma60, "momentum_5d": round(momentum_5d, 2)}
    except Exception as e:
        _logger.info(f"[_get_real_ma] 补调日线失败 {code}: {e}")
        return {}


def _get_mx_financial_supplement(code: str, name: str) -> dict:
    """
    补调 MX mx_data 查营收增长率和经营现金流。
    返回 {"revenue_growth": float, "cash_flow_positive": bool}
    失败返回空 dict。
    """
    try:
        from scripts.mx.mx_data import MXData
        mx = MXData()
        result = mx.query(f"{name} 最近一期营收同比增长率 经营活动现金流净额")
        tables, _, _, err = MXData.parse_result(result)
        if err or not tables:
            return {}
        parsed = {}
        for table in tables:
            for row in table.get("rows", []):
                for key, val in row.items():
                    val_str = str(val).strip().replace("%", "").replace(",", "")
                    if "|" in val_str:
                        val_str = val_str.split("|")[0]
                    try:
                        num = float(val_str) if val_str and val_str not in ("", "-", "—") else None
                    except (ValueError, TypeError):
                        num = None
                    if num is None:
                        continue
                    if "营收" in key and ("增长" in key or "同比" in key):
                        parsed.setdefault("revenue_growth", round(num, 2))
                    elif "经营" in key and "现金" in key:
                        parsed.setdefault("cash_flow_positive", num > 0)
                        parsed.setdefault("operating_cash_flow", num)
        if parsed:
            _logger.info(f"[mx_fin_sup] {name}: rev={parsed.get('revenue_growth')} cf={parsed.get('cash_flow_positive')}")
        return parsed
    except Exception as e:
        _logger.info(f"[mx_fin_sup] {name} 失败: {e}")
        return {}


def _check_earnings_bomb(code: str, name: str) -> bool:
    """
    业绩暴雷检测：查最近业绩预告/快报，净利润同比下滑>30%或由盈转亏 → True（暴雷）。
    """
    try:
        from scripts.mx.mx_data import MXData
        mx = MXData()
        result = mx.query(f"{name} 最近一期净利润同比增长率")
        tables, _, _, err = MXData.parse_result(result)
        if err or not tables:
            return False
        for table in tables:
            for row in table.get("rows", []):
                for key, val in row.items():
                    if "净利润" in key and ("增长" in key or "同比" in key):
                        val_str = str(val).strip().replace("%", "").replace(",", "")
                        if "|" in val_str:
                            val_str = val_str.split("|")[0]
                        try:
                            growth = float(val_str)
                            if growth <= -30:
                                _logger.info(f"[earnings_bomb] {name} 净利润同比{growth:.1f}% → 暴雷")
                                return True
                        except (ValueError, TypeError):
                            pass
        return False
    except Exception:
        return False


def _score_from_mx_data(code: str, name: str, mx_row: dict,
                         skip_sentiment: bool = False) -> dict:
    """
    用妙想选股返回的行数据 + 补调数据做四维评分（快速模式）。

    优化项：
      1. 均线排列：补调 MX 日线计算 MA5/MA20/MA60 真实排列
      2. 基本面：补调 MX 查营收增长率和现金流
      3. 资金流：用换手率替代成交额绝对值
      4. 业绩暴雷检测：净利润同比下滑>30% → 一票否决
    """
    strategy = get_strategy()
    weights = strategy.get("scoring", {}).get("weights", {})

    # ── 提取妙想选股返回的字段 ──
    price = _parse_mx_float(mx_row, "最新价", "收盘价")
    ma20_mx = _parse_mx_float(mx_row, "20日均线")
    chg_pct = _parse_mx_float(mx_row, "涨跌幅")
    turnover = _parse_mx_float(mx_row, "换手率")
    vol_ratio = _parse_mx_float(mx_row, "量比")
    roe = _parse_mx_float(mx_row, "ROE", "净资产收益率")
    pe = _parse_mx_float(mx_row, "市盈率")
    amount = _parse_mx_float(mx_row, "成交额")
    circ_mv = _parse_mx_float(mx_row, "流通市值")

    above_ma20 = price > ma20_mx if ma20_mx > 0 else True

    # ── 优化1: 补调日线算真实均线排列 + 5日动量 ──
    ma_info = _get_real_ma_arrangement(code, name)
    ma5 = ma_info.get("ma5")
    ma20_real = ma_info.get("ma20", ma20_mx)
    ma60 = ma_info.get("ma60")
    momentum_5d = ma_info.get("momentum_5d", chg_pct)

    # 用真实 MA20 重新判断
    if ma20_real and ma20_real > 0:
        above_ma20 = price > ma20_real

    # ── 技术面评分（满分 2）──
    ma_score = 0.5 if above_ma20 and ma60 and price >= ma60 else (0.3 if above_ma20 else 0)

    # 量比按市值分档
    if circ_mv > 0:
        if circ_mv > 200e8:
            vol_threshold = 1.2
        elif circ_mv > 50e8:
            vol_threshold = 1.5
        else:
            vol_threshold = 1.8
    else:
        vol_threshold = 1.5
    vol_score = 0.5 if vol_ratio >= vol_threshold else (0.3 if vol_ratio >= 1.0 else 0.1)

    # 动量用 5 日涨幅（真实值）
    if momentum_5d >= 5:
        mom_score = 0.5
    elif momentum_5d >= 2:
        mom_score = 0.3
    elif momentum_5d >= 0:
        mom_score = 0.1
    else:
        mom_score = 0

    # 均线排列（真实计算）
    arr_score = 0
    if ma5 and ma20_real and ma60:
        if ma5 > ma20_real > ma60:
            arr_score = 0.5
        elif ma20_real > ma60:
            arr_score = 0.3
    elif above_ma20:
        arr_score = 0.2  # 无法计算时给低分而非中性分

    tech_total = round(min(ma_score + vol_score + mom_score + arr_score, 2.0), 1)
    tech_detail = (f"均线:{ma_score}/0.5 量比:{vol_score}/0.5(阈值{vol_threshold}) "
                   f"动量:{mom_score}/0.5(5d:{momentum_5d:+.1f}%) 排列:{arr_score}/0.5"
                   f"{'(MA5>20>60)' if arr_score == 0.5 else ''}")

    # ── 优化2: 基本面补调营收/现金流 ──
    fin_sup = _get_mx_financial_supplement(code, name)

    if roe >= 15:
        roe_score = 1.0
    elif roe >= 10:
        roe_score = 0.7
    elif roe >= 5:
        roe_score = 0.4
    else:
        roe_score = 0

    rev_growth = fin_sup.get("revenue_growth")
    if rev_growth is not None:
        if rev_growth >= 20:
            rev_score = 1.0
        elif rev_growth >= 10:
            rev_score = 0.7
        elif rev_growth >= 0:
            rev_score = 0.3
        else:
            rev_score = 0
        rev_detail = f"{rev_score:.1f}/1({rev_growth:+.1f}%)"
    else:
        rev_score = 0.5
        rev_detail = "0.5/1(无数据)"

    cf_positive = fin_sup.get("cash_flow_positive")
    if cf_positive is not None:
        cf_score = 0.5 if cf_positive else 0
        cf_detail = f"{cf_score:.1f}/1({'正' if cf_positive else '负'})"
    else:
        cf_score = 0.5
        cf_detail = "0.5/1(无数据)"

    fin_total = round(min(roe_score + rev_score + cf_score, 3.0), 1)
    fin_detail = f"ROE:{roe_score:.1f}/1({roe:.1f}%) 营收:{rev_detail} 现金流:{cf_detail}"

    # ── 优化3: 资金流用换手率替代成交额绝对值 ──
    if turnover >= 3 and turnover <= 10:
        flow_score_val = 1.0
    elif turnover > 10:
        flow_score_val = 0.7  # 异常放量警惕
    elif turnover >= 1:
        flow_score_val = 0.4
    else:
        flow_score_val = 0.2
    north_score = 0.5  # 北向无法从选股结果获取
    flow_total = round(min(flow_score_val + north_score, 2.0), 1)
    flow_detail = f"换手率:{flow_score_val:.1f}/1({turnover:.1f}%) 北向:0.5/1(无数据)"

    # ── 舆情评分 ──
    if skip_sentiment:
        sentiment = {"score": 1.5, "detail": "跳过（非前N名）", "sentiment": "neutral"}
    else:
        sentiment = _score_sentiment(code, name)

    # ── 一票否决 ──
    veto_signals = []
    if not above_ma20:
        veto_signals.append("below_ma20")
    if abs(chg_pct) >= 9.9:
        veto_signals.append("limit_up_today")

    # 优化4: 业绩暴雷检测
    if _check_earnings_bomb(code, name):
        veto_signals.append("earnings_bomb")

    veto_triggered = len(veto_signals) > 0

    # ── 加权总分 ──
    raw = (
        tech_total * weights.get("technical", 2) / 2 +
        fin_total * weights.get("fundamental", 3) / 3 +
        flow_total * weights.get("flow", 2) / 2 +
        sentiment["score"] * weights.get("sentiment", 3) / 3
    )
    total = 0.0 if veto_triggered else round(raw, 1)

    return {
        "name": name,
        "code": code,
        "technical_score": tech_total,
        "fundamental_score": fin_total,
        "flow_score": flow_total,
        "sentiment_score": sentiment["score"],
        "total_score": total,
        "technical_detail": tech_detail,
        "fundamental_detail": fin_detail,
        "flow_detail": flow_detail,
        "sentiment_detail": sentiment["detail"],
        "veto_signals": veto_signals,
        "veto_triggered": veto_triggered,
        "weights": weights,
    }


def batch_score(stocks: list) -> list:
    """
    批量评分

    Args:
        stocks: list of {"code": str, "name": str, "mx_data": dict (可选)}
        如果 mx_data 存在，走快速模式（补调日线+财务+业绩暴雷检测）；
        否则走传统的逐接口拉取模式。

    优化：舆情搜索只对三维排名前 8 名执行，其余给默认 1.5 分。

    Returns:
        评分结果列表（按总分降序）
    """
    SENTIMENT_TOP_N = 8  # 只对前 N 名做舆情搜索

    results = []
    has_mx_data = any(item.get("mx_data") for item in stocks)

    if has_mx_data:
        # ── 快速模式：两轮评分 ──
        # 第一轮：技术+基本面+资金（跳过舆情），快速排序
        first_pass = []
        for item in stocks:
            code = normalize_code(item.get("code", ""))
            name = item.get("name")
            mx_data = item.get("mx_data")
            try:
                if mx_data:
                    r = _score_from_mx_data(code, name, mx_data, skip_sentiment=True)
                else:
                    r = score(code, name)
                first_pass.append(r)
            except Exception as e:
                _logger.error(f"[scorer] 第一轮评分失败 {code}: {e}")
                first_pass.append({
                    "code": code, "name": name or code,
                    "total_score": 0, "veto_triggered": True,
                    "veto_signals": ["score_error"], "error": str(e),
                })

        # 按三维分数排序（不含舆情）
        first_pass.sort(key=lambda x: x.get("total_score", 0), reverse=True)

        # 第二轮：只对前 N 名补充舆情搜索
        for i, r in enumerate(first_pass):
            code = r.get("code", "")
            name = r.get("name", "")
            if r.get("veto_triggered"):
                results.append(r)
                _logger.info(
                    f"[scorer] {name}({code}): "
                    f"技术{r.get('technical_score',0):.1f} 基本面{r.get('fundamental_score',0):.1f} "
                    f"资金{r.get('flow_score',0):.1f} 舆情1.5(跳过) "
                    f"→ 总分0.0 ❌ veto:{','.join(r.get('veto_signals',[]))}"
                )
                continue

            if i < SENTIMENT_TOP_N:
                # 补充舆情评分
                sentiment = _score_sentiment(code, name)
                r["sentiment_score"] = sentiment["score"]
                r["sentiment_detail"] = sentiment["detail"]
                # 重算总分
                weights = r.get("weights", get_strategy().get("scoring", {}).get("weights", {}))
                raw = (
                    r.get("technical_score", 0) * weights.get("technical", 2) / 2 +
                    r.get("fundamental_score", 0) * weights.get("fundamental", 3) / 3 +
                    r.get("flow_score", 0) * weights.get("flow", 2) / 2 +
                    sentiment["score"] * weights.get("sentiment", 3) / 3
                )
                r["total_score"] = round(raw, 1)

            results.append(r)
            _logger.info(
                f"[scorer] {name}({code}): "
                f"技术{r.get('technical_score',0):.1f} 基本面{r.get('fundamental_score',0):.1f} "
                f"资金{r.get('flow_score',0):.1f} 舆情{r.get('sentiment_score',0):.1f} "
                f"→ 总分{r.get('total_score',0):.1f}"
                + (f" {'(舆情已评)' if i < SENTIMENT_TOP_N else '(舆情默认)'}")
            )

        results.sort(key=lambda x: x.get("total_score", 0), reverse=True)
        return results

    # ── 传统模式 ──
    for item in stocks:
        code = normalize_code(item.get("code", ""))
        name = item.get("name")
        try:
            code = normalize_code(code)
            strategy = get_strategy()
            weights = strategy.get("scoring", {}).get("weights", {})

            tech = get_technical(code, 60)
            tech_s = _score_technical(code, tech)

            if not tech.get("above_ma20", True):
                r = {
                    "name": name or tech.get("name", code),
                    "code": code,
                    "technical_score": tech_s["score"],
                    "fundamental_score": 0, "flow_score": 0, "sentiment_score": 1.5,
                    "total_score": 0,
                    "technical_detail": tech_s["detail"],
                    "fundamental_detail": "跳过（below_ma20 veto）",
                    "flow_detail": "跳过", "sentiment_detail": "跳过",
                    "veto_signals": ["below_ma20"], "veto_triggered": True,
                    "weights": weights,
                }
                results.append(r)
                _logger.info(f"[scorer] {r['name']}({code}): 技术{tech_s['score']:.1f} → 总分0.0 ❌ veto:below_ma20")
                continue

            r = score(code, name)
            results.append(r)
            _logger.info(
                f"[scorer] {r['name']}({code}): "
                f"技术{r.get('technical_score',0):.1f} 基本面{r.get('fundamental_score',0):.1f} "
                f"资金{r.get('flow_score',0):.1f} 舆情{r.get('sentiment_score',0):.1f} "
                f"→ 总分{r.get('total_score',0):.1f}"
                + (f" ❌ veto:{','.join(r.get('veto_signals',[]))}" if r.get('veto_triggered') else "")
            )
        except Exception as e:
            _logger.error(f"[scorer] 评分失败 {code}: {e}")
            results.append({
                "code": code, "name": name or code,
                "total_score": 0, "veto_triggered": True,
                "veto_signals": ["score_error"], "error": str(e),
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
