"""
pipeline/sentiment.py — 舆情监控

流程：
1. 读持仓股 + 核心池股票
2. 对每只票调 mx-search 搜最新资讯
3. 过滤重要事件（评级变动、重大公告）
4. 去重（跟已推送的比对）
5. 写事件 + Obsidian 日志
6. 格式化 Discord embed 推送
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from datetime import datetime, timezone

from hermes.pipeline.context import PipelineContext

_logger = logging.getLogger(__name__)

# 重要事件的关键词
_RATING_KEYWORDS = {"买入", "增持", "推荐", "强烈推荐", "优于大市"}
_NEGATIVE_KEYWORDS = {"减持", "卖出", "回避", "中性"}
_EVENT_KEYWORDS = {"业绩预告", "业绩快报", "重大合同", "股权变动", "回购", "增减持",
                   "停牌", "退市", "立案", "处罚", "诉讼", "违规"}


def _item_hash(item: dict) -> str:
    """生成资讯去重 hash。"""
    key = f"{item.get('code', '')}{item.get('title', '')}"
    return hashlib.md5(key.encode()).hexdigest()[:16]


def _classify_item(item: dict) -> dict | None:
    """
    分类资讯，返回 None 表示不重要（跳过）。
    返回 dict 包含 level/emoji/summary。
    """
    info_type = item.get("informationType", "")
    rating = str(item.get("rating", ""))
    title = item.get("title", "")
    content = item.get("content", "")[:200]
    ins_name = item.get("insName", "")
    entity = item.get("entityFullName", "")

    # 研报评级
    if info_type == "REPORT" and rating:
        if any(k in rating for k in _RATING_KEYWORDS):
            return {
                "level": "positive",
                "emoji": "🟢",
                "summary": f"{ins_name}发布研报「{rating}」评级",
            }
        if any(k in rating for k in _NEGATIVE_KEYWORDS):
            return {
                "level": "negative",
                "emoji": "🔴",
                "summary": f"{ins_name}发布研报「{rating}」评级",
            }

    # 重大公告
    if info_type == "ANNOUNCEMENT":
        for kw in _EVENT_KEYWORDS:
            if kw in title or kw in content:
                return {
                    "level": "event",
                    "emoji": "📢",
                    "summary": title[:60],
                }

    # 负面新闻
    if info_type == "NEWS":
        neg_words = {"暴跌", "跌停", "爆雷", "违规", "处罚", "退市", "立案", "亏损"}
        for w in neg_words:
            if w in title:
                return {
                    "level": "negative",
                    "emoji": "🔴",
                    "summary": title[:60],
                }

    return None


def _get_pushed_hashes(conn, hours: int = 24) -> set[str]:
    """获取最近已推送的资讯 hash。"""
    try:
        rows = conn.execute(
            "SELECT payload_json FROM market_observations "
            "WHERE source = 'sentiment_monitor' AND kind = 'news_alert' "
            "ORDER BY observed_at DESC LIMIT 500"
        ).fetchall()
        hashes = set()
        for r in rows:
            try:
                p = json.loads(r["payload_json"])
                hashes.add(p.get("hash", ""))
            except Exception:
                pass
        return hashes
    except Exception:
        return set()


def _save_pushed(conn, symbol: str, item_hash: str, summary: str, run_id: str):
    """记录已推送的资讯。"""
    import uuid
    now = datetime.now(timezone.utc).isoformat()
    try:
        conn.execute(
            """INSERT OR IGNORE INTO market_observations
               (observation_id, source, kind, symbol, observed_at, run_id, payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (uuid.uuid4().hex[:16], "sentiment_monitor", "news_alert",
             symbol, now, run_id,
             json.dumps({"hash": item_hash, "summary": summary}, ensure_ascii=False)),
        )
    except Exception as e:
        _logger.warning(f"[sentiment] save_pushed failed: {e}")


def run(ctx: PipelineContext, run_id: str) -> dict:
    """执行舆情监控 pipeline。"""
    from hermes.market.mx.search import MXSearch

    # 1. 收集监控股票列表（持仓 + 核心池，去重）
    positions = ctx.exec_svc.get_positions()
    pool_rows = ctx.conn.execute(
        "SELECT code, name FROM projection_candidate_pool WHERE pool_tier = 'core' ORDER BY score DESC"
    ).fetchall()

    watch_stocks: dict[str, str] = {}  # code → name
    for p in positions:
        watch_stocks[p.code] = p.name
    for r in pool_rows:
        if r["code"] not in watch_stocks:
            watch_stocks[r["code"]] = r["name"] or r["code"]

    if not watch_stocks:
        _logger.info("[sentiment] 无持仓和核心池，跳过")
        return {"monitored": 0, "alerts": []}

    # 2. 获取已推送 hash
    pushed_hashes = _get_pushed_hashes(ctx.conn)

    # 3. 逐票搜索资讯
    mx = MXSearch()
    alerts = []

    for code, name in watch_stocks.items():
        try:
            result = mx.search(f"{name} 最新资讯")
            items = mx.extract_items(result)
        except Exception as e:
            _logger.warning(f"[sentiment] {name}({code}) search failed: {e}")
            continue

        for item in items:
            h = _item_hash(item)
            if h in pushed_hashes:
                continue

            classified = _classify_item(item)
            if classified is None:
                continue

            alert = {
                "code": code,
                "name": name,
                "hash": h,
                "date": item.get("date", "")[:10],
                **classified,
            }
            alerts.append(alert)

            # 记录已推送
            _save_pushed(ctx.conn, code, h, classified["summary"], run_id)
            pushed_hashes.add(h)

    # 4. Obsidian 日志
    if alerts:
        lines = ["## 舆情监控", ""]
        for a in alerts:
            lines.append(f"- {a['emoji']} {a['name']}({a['code']}): {a['summary']}")
        ctx.obsidian.write_daily_log(run_id, "\n".join(lines))

    # 5. Discord 推送
    if alerts:
        from hermes.reporting.discord import format_sentiment_embed
        embed = format_sentiment_embed(alerts)
        try:
            from hermes.reporting.discord_sender import send_embed
            ok, err = send_embed(embed)
            if not ok:
                _logger.warning(f"[sentiment] Discord 推送失败: {err}")
        except Exception as e:
            _logger.warning(f"[sentiment] Discord 推送异常: {e}")

    _logger.info(f"[sentiment] 完成: 监控 {len(watch_stocks)} 只, {len(alerts)} 条告警")

    return {
        "monitored": len(watch_stocks),
        "alerts": alerts,
    }
