"""
pipeline/scoring.py — 核心池评分

流程：
1. 读核心池股票列表
2. MarketService 批量抓取数据
3. 抓大盘信号
4. StrategyService 评分 + 决策 → 事件写入
5. 更新 projection_candidate_pool
6. 写 Obsidian（评分报告 + 核心池 + 观察池）
7. 格式化 Discord embed
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date

from hermes.pipeline.context import PipelineContext
from hermes.reporting.discord import format_scoring_embed

_logger = logging.getLogger(__name__)


def run(ctx: PipelineContext, run_id: str) -> dict:
    """执行核心池评分 pipeline。"""

    # 1. 读核心池
    rows = ctx.conn.execute(
        "SELECT code, name FROM projection_candidate_pool WHERE pool_tier = 'core' ORDER BY score DESC"
    ).fetchall()
    stock_list = [{"code": r["code"], "name": r["name"] or ""} for r in rows]

    if not stock_list:
        _logger.warning("[scoring] 核心池为空")
        return {"scored": 0, "stock_list": []}

    # 2. 批量抓取
    snapshots = asyncio.run(ctx.market_svc.collect_batch(stock_list, run_id))

    # 3. 大盘信号
    market_state = asyncio.run(ctx.market_svc.collect_market_state(run_id))

    # 4. 评分 + 决策
    decisions = ctx.strategy_svc.evaluate(
        snapshots, market_state, run_id, ctx.config_version,
    )

    # 收集评分结果
    score_events = ctx.event_store.query(event_type="score.calculated")
    run_scores = [e["payload"] for e in score_events if e.get("metadata", {}).get("run_id") == run_id]
    run_scores.sort(key=lambda x: x.get("total_score", 0), reverse=True)

    # 5. 更新 projection_candidate_pool
    pool_entries = []
    for s in run_scores:
        total = s.get("total_score", 0)
        pool_entries.append({
            "code": s.get("code", ""),
            "name": s.get("name", ""),
            "pool_tier": "core",
            "score": total,
            "note": "veto" if s.get("veto_triggered") else "",
        })
    ctx.projector.sync_candidate_pool(pool_entries)

    # 6. Obsidian
    ctx.obsidian.write_scoring_report(run_id, run_scores)
    ctx.obsidian.write_core_pool()
    ctx.obsidian.write_watch_pool()

    # 日志追加
    lines = [f"## 核心池评分", ""]
    for s in run_scores:
        total = s.get("total_score", 0)
        emoji = "✅" if total >= 7 else ("🟡" if total >= 5 else "❌")
        lines.append(f"- {s.get('name', '')}({s.get('code', '')}) {emoji} {total:.1f}")
    ctx.obsidian.write_daily_log(run_id, "\n".join(lines))

    # 7. Discord embed
    embed = format_scoring_embed(run_scores, date.today().isoformat())

    _logger.info(f"[scoring] 完成: {len(run_scores)} 只评分")

    # 8. Discord 推送
    try:
        from hermes.reporting.discord_sender import send_embed
        ok, err = send_embed(embed)
        if not ok:
            _logger.warning(f"[scoring] Discord 推送失败: {err}")
    except Exception as e:
        _logger.warning(f"[scoring] Discord 推送异常: {e}")

    return {
        "scored": len(run_scores),
        "scores": run_scores,
        "discord_embed": embed,
    }
