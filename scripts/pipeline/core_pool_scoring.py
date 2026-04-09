#!/usr/bin/env python3
"""
pipeline/core_pool_scoring.py — 核心池每日评分（15:40 执行）

职责：
  1. 读 config/stocks.yaml 的核心池列表
  2. 批量拉取：实时价格 + 技术指标 + 基本面 + 资金流向 + TrendRadar 舆情
  3. 四维评分（技术/基本面/资金/舆情）
  4. 输出到 vault/04-选股/评分报告/核心池_评分_YYYYMMDD.md
  5. 更新 vault/04-选股/核心池.md 的评分列

用法（CLI）：
  python -m scripts.pipeline.core_pool_scoring

用法（导入）：
  from scripts.pipeline.core_pool_scoring import run
  scores = run()  # 返回评分列表
"""

import os
import sys
import warnings
from datetime import datetime
from pathlib import Path

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

os.environ["TQDM_DISABLE"] = "1"
warnings.filterwarnings("ignore")

import pandas as pd
from scripts.engine.data_engine import DataEngine
from scripts.utils.obsidian import ObsidianVault
from scripts.utils.config_loader import get_strategy, get_stocks
from scripts.utils.logger import get_logger

_logger = get_logger("pipeline.core_pool_scoring")


# ---------------------------------------------------------------------------
# 四维评分
# ---------------------------------------------------------------------------

def _score_technical(engine: DataEngine, code: str) -> dict:
    """
    技术面评分（满分 2 分）
    维度：MA20/MA60 趋势（0.5）+ 成交量（0.5）+ 动量（0.5）+ 均线排列（0.5）
    """
    tech = engine.get_technical(code, 60)
    if "error" in tech:
        return {"score": 0, "detail": f"数据错误: {tech['error']}"}

    price = tech.get("current_price", 0)
    ma = tech.get("ma", {})
    ma20 = ma.get("MA20", 0)
    ma60 = ma.get("MA60", 0)

    # 均线趋势得分（0.5）
    ma_score = 0
    if ma20 and ma60:
        if price >= ma20 and price >= ma60:
            ma_score = 0.5  # 价格站上双均线
        elif price >= ma20:
            ma_score = 0.3  # 仅站上 MA20
        elif price < ma20:
            ma_score = 0   # 跌破 MA20

    # 成交量得分（0.5）- 简化：量比 > 1.2 得 0.5
    vol_score = 0
    vol_ratio = tech.get("volume_ratio", 1)
    if vol_ratio >= 1.5:
        vol_score = 0.5
    elif vol_ratio >= 1.2:
        vol_score = 0.3
    elif vol_ratio >= 1.0:
        vol_score = 0.1

    # 动量得分（0.5）- 近5日涨幅
    momentum = tech.get("momentum_5d", 0)
    if momentum >= 5:
        mom_score = 0.5
    elif momentum >= 2:
        mom_score = 0.3
    elif momentum >= 0:
        mom_score = 0.1
    else:
        mom_score = 0

    # 均线排列得分（0.5）- MA5>MA20>MA60 得多分
    ma5 = ma.get("MA5", 0)
    arr_score = 0
    if ma5 and ma20 and ma60:
        if ma5 > ma20 > ma60:
            arr_score = 0.5
        elif ma20 > ma60:
            arr_score = 0.3
        elif ma5 < ma20:
            arr_score = 0

    total = min(ma_score + vol_score + mom_score + arr_score, 2.0)
    return {
        "score": round(total, 1),
        "detail": f"均线:{ma_score}/0.5 量:{vol_score}/0.5 动量:{mom_score}/0.5 排列:{arr_score}/0.5",
        "price": price,
        "ma20": ma20,
        "above_ma20": price >= ma20 if ma20 else False,
    }


def _score_fundamental(engine: DataEngine, code: str) -> dict:
    """
    基本面评分（满分 3 分）
    维度：ROE（1）+ 营收增长（1）+ 现金流（1）
    """
    fin = engine.get_financial(code)
    if "error" in fin:
        return {"score": 0, "detail": f"数据错误: {fin['error']}"}

    # ROE（0-1 分）
    roe = fin.get("roe", 0)
    if roe >= 15:
        roe_score = 1.0
    elif roe >= 10:
        roe_score = 0.7
    elif roe >= 5:
        roe_score = 0.4
    else:
        roe_score = 0

    # 营收增长（0-1 分）
    rev_growth = fin.get("revenue_growth", 0)
    if rev_growth >= 20:
        rev_score = 1.0
    elif rev_growth >= 10:
        rev_score = 0.7
    elif rev_growth >= 0:
        rev_score = 0.3
    else:
        rev_score = 0

    # 现金流（0-1 分）
    cf_score = 0.5 if fin.get("operating_cash_flow", 0) > 0 else 0

    total = min(roe_score + rev_score + cf_score, 3.0)
    return {
        "score": round(total, 1),
        "detail": f"ROE:{roe_score:.1f}/1 营收:{rev_score:.1f}/1 现金流:{cf_score:.1f}/1",
        "roe": roe,
        "revenue_growth": rev_growth,
    }


def _score_fund_flow(engine: DataEngine, code: str) -> dict:
    """
    资金流评分（满分 2 分）
    维度：主力净流入（1）+ 北向持股变化（1）
    """
    flow = engine.get_fund_flow(code)
    if "error" in flow:
        return {"score": 0, "detail": f"数据错误: {flow['error']}"}

    # 主力净流入（0-1 分）
    main_net = flow.get("main_net_inflow", 0)
    if main_net > 1_000_000_000:
        main_score = 1.0
    elif main_net > 500_000_000:
        main_score = 0.7
    elif main_net > 0:
        main_score = 0.4
    else:
        main_score = 0

    # 北向资金（简化：持股比例增加为正面）
    south_score = 0.5  # 默认中性

    total = min(main_score + south_score, 2.0)
    return {
        "score": round(total, 1),
        "detail": f"主力:{main_score}/1.0 北向:{south_score}/1.0",
        "main_net_inflow": main_net,
        "main_outflow": main_net < 0,
    }


def _score_sentiment(code: str, name: str) -> dict:
    """
    舆情评分（满分 3 分）
    优先使用妙想资讯搜索获取研报/新闻。
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

        score = 1.5
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
        return {"score": 1.5, "detail": "MX搜索失败，默认1.5分", "sentiment": "neutral"}


def _get_total_score(tech_score: float, fin_score: float,
                      flow_score: float, sentiment_score: float,
                      veto_signals: list) -> float:
    """计算总分（10分制），应用 veto 规则"""
    strategy = get_strategy()
    weights = strategy.get("scoring", {}).get("weights", {})

    # 各维度权重归一化（满分 10）
    raw = (
        tech_score * weights.get("technical", 2) / 2 +
        fin_score * weights.get("fundamental", 3) / 3 +
        flow_score * weights.get("flow", 2) / 2 +
        sentiment_score * weights.get("sentiment", 3) / 3
    )
    total = round(raw, 1)

    # veto 检查
    veto = strategy.get("scoring", {}).get("veto", [])
    for signal in veto_signals:
        if signal in veto:
            return 0.0

    return total


def _build_report_content(scores: list, date_str: str) -> str:
    """生成评分报告 markdown 内容"""
    weekday_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    weekday = weekday_names[dt.weekday()]

    lines = [
        f"# 核心池评分报告 — {date_str}（{weekday}）",
        "",
        f"评分时间：{datetime.now().strftime('%H:%M')}",
        "",
        "---",
        "",
        "| 股票 | 代码 | 技术(2) | 基本面(3) | 资金(2) | 舆情(3) | **总分(10)** | 建议 |",
        "|------|------|---------|---------|---------|---------|------------|------|",
    ]

    for s in scores:
        name = s.get("name", "")
        code = s.get("code", "")
        tech = s.get("technical_score", 0)
        fin = s.get("fundamental_score", 0)
        flow = s.get("flow_score", 0)
        sentiment = s.get("sentiment_score", 0)
        total = s.get("total_score", 0)
        veto_signals = s.get("veto_signals", [])

        if veto_signals:
            suggestion = "❌ 一票否决"
        elif total >= 7:
            suggestion = "✅ 可买入"
        elif total >= 5:
            suggestion = "🟡 观察"
        else:
            suggestion = "❌ 规避"

        lines.append(
            f"| {name} | {code} | {tech:.1f} | {fin:.1f} | {flow:.1f} | "
            f"{sentiment:.1f} | **{total:.1f}** | {suggestion} |"
        )

    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 详细数据")
    lines.append("")

    for s in scores:
        lines.append(f"### {s.get('name', '')}（{s.get('code', '')}）")
        lines.append(f"- **总分：{s.get('total_score', 0):.1f}**")
        lines.append(f"- 技术面：{s.get('technical_detail', '')}")
        lines.append(f"- 基本面：{s.get('fundamental_detail', '')}")
        lines.append(f"- 资金流：{s.get('flow_detail', '')}")
        lines.append(f"- 舆情：{s.get('sentiment_detail', '')}")
        if s.get("veto_signals"):
            lines.append(f"- **一票否决：{', '.join(s['veto_signals'])}**")
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------

def run() -> list:
    """
    执行核心池评分

    Returns:
        list of dict，每个元素包含评分详情
    """
    date_str = datetime.now().strftime("%Y-%m-%d")
    _logger.info(f"[SCORING] 核心池评分 {date_str}")

    vault = ObsidianVault()
    engine = DataEngine()
    stocks_cfg = get_stocks()

    # 读取核心池列表
    core_pool = stocks_cfg.get("core_pool", [])
    if not core_pool:
        _logger.warning("核心池为空，从 vault 读取")
        vault_pool = vault.read_core_pool()
        core_pool = [
            {"code": str(row.get("代码", "")).strip(), "name": str(row.get("股票", "")).strip()}
            for row in vault_pool
            if str(row.get("代码", "")).strip() not in ["", "—"]
        ]

    if not core_pool:
        _logger.warning("核心池为空，退出")
        return []

    _logger.info(f">> 核心池: {len(core_pool)} 只")

    scores = []
    for item in core_pool:
        code = str(item.get("code", "")).strip()
        name = str(item.get("name", "")).strip()
        if not code:
            continue

        _logger.info(f">> 评分 {name}({code})...")
        try:
            tech = _score_technical(engine, code)
            fin = _score_fundamental(engine, code)
            flow = _score_fund_flow(engine, code)
            sentiment = _score_sentiment(code, name)

            # 收集 veto 信号
            veto_signals = []
            if not tech.get("above_ma20", True):
                veto_signals.append("below_ma20")
            if flow.get("main_outflow", False):
                veto_signals.append("consecutive_outflow")

            total = _get_total_score(
                tech["score"], fin["score"],
                flow["score"], sentiment["score"],
                veto_signals
            )

            score_entry = {
                "name": name,
                "code": code,
                "technical_score": tech["score"],
                "fundamental_score": fin["score"],
                "flow_score": flow["score"],
                "sentiment_score": sentiment["score"],
                "total_score": total,
                "technical_detail": tech["detail"],
                "fundamental_detail": fin["detail"],
                "flow_detail": flow["detail"],
                "sentiment_detail": sentiment["detail"],
                "veto_signals": veto_signals,
            }
            scores.append(score_entry)

            veto_msg = f" ❌ veto:{','.join(veto_signals)}" if veto_signals else ""
            _logger.info(
                f"  {name}: 技术{tech['score']:.1f} 基本面{fin['score']:.1f} "
                f"资金{flow['score']:.1f} 舆情{sentiment['score']:.1f} "
                f"→ 总分{total:.1f}{veto_msg}"
            )

        except Exception as e:
            _logger.error(f"  {name}({code}) 评分失败: {e}")
            scores.append({
                "name": name,
                "code": code,
                "technical_score": 0,
                "fundamental_score": 0,
                "flow_score": 0,
                "sentiment_score": 0,
                "total_score": 0,
                "veto_signals": ["score_error"],
            })

    # 写入评分报告
    _logger.info(">> 写入评分报告...")
    report_content = _build_report_content(scores, date_str)
    report_dir = Path(vault.vault_path) / "04-选股" / "筛选结果"
    report_dir.mkdir(parents=True, exist_ok=True)
    time_str = datetime.now().strftime("%H%M%S")
    report_path = report_dir / f"核心池_评分报告_{date_str.replace('-', '')}_{time_str}.md"
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report_content)
    _logger.info(f"  已写入: {report_path.name}")

    # 更新核心池.md 的评分列
    _logger.info(">> 更新核心池.md 评分列...")
    try:
        vault.update_core_pool_scores(scores)
        _logger.info("  核心池.md 已更新")
    except Exception as e:
        _logger.warning(f"  更新核心池.md 失败: {e}")

    _logger.info(f"[SCORING] 评分完成，共 {len(scores)} 只")

    return scores


if __name__ == "__main__":
    import pandas as pd
    result = run()
    print(f"\n核心池评分完成，共 {len(result)} 只")
