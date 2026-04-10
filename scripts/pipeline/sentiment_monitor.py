#!/usr/bin/env python3
"""
pipeline/sentiment_monitor.py — 舆情独立监控

定时扫描核心池 + 持仓相关资讯，命中关键词后走告警中心 → Discord 推送。

功能：
  - 扫描核心池 + 观察池 + 持仓股票的最新资讯
  - 关键词匹配（财报暴雷/重大利空/监管处罚/高管变动等）
  - 命中后生成告警 → Discord 推送
  - 去重：同一 alert_key 24h 内不重复推送

用法：
  python -m scripts.pipeline.sentiment_monitor           # 扫描并推送
  python -m scripts.pipeline.sentiment_monitor --dry-run  # 只扫描不推送
  bin/trade run sentiment --json

CLI 集成：
  bin/trade run sentiment --json
"""

import json
import os
import sys
import re
from datetime import datetime, timedelta
from pathlib import Path

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from scripts.mx.cli_tools import MXCommandError, dispatch_mx_command
from scripts.utils.config_loader import get_stocks, get_strategy
from scripts.utils.discord_push import send_sentiment_alert
from scripts.utils.cache import load_json_cache, save_json_cache
from scripts.utils.logger import get_logger

_logger = get_logger("pipeline.sentiment_monitor")

# ---------------------------------------------------------------------------
# 关键词配置
# ---------------------------------------------------------------------------

# 负面关键词 → 告警级别
NEGATIVE_KEYWORDS = {
    "high": [
        "暴雷", "爆雷", "财务造假", "立案调查", "退市", "ST",
        "重大亏损", "业绩变脸", "净利润.*下滑", "净利润.*下降",
        "监管处罚", "行政处罚", "证监会.*处罚", "违规",
        "高管被查", "实控人.*被", "董事长.*辞职",
        "停牌", "暂停上市", "终止上市",
        "商誉减值", "资产减值",
    ],
    "warning": [
        "减持", "大股东减持", "高管减持", "解禁",
        "下调评级", "卖出评级",
        "诉讼", "仲裁", "纠纷",
        "产能过剩", "行业下行",
        "质押.*平仓", "股权质押",
        "业绩不及预期", "低于预期",
    ],
}

# 正面关键词（仅记录，不告警）
POSITIVE_KEYWORDS = [
    "上调评级", "买入评级", "增持评级",
    "业绩超预期", "净利润.*增长", "营收.*增长",
    "大单", "中标", "签约",
    "回购", "增持",
]

CACHE_KEY = "sentiment_alert_history"
DEDUP_HOURS = 24


# ---------------------------------------------------------------------------
# 核心逻辑
# ---------------------------------------------------------------------------

def _get_watch_stocks() -> list[dict]:
    """获取需要监控的股票列表：核心池 + 观察池 + 持仓"""
    stocks_cfg = get_stocks()
    watch_list = []
    seen_codes = set()

    for pool_key in ("core_pool", "watch_pool"):
        for item in stocks_cfg.get(pool_key, []):
            code = str(item.get("code", "")).strip()
            name = str(item.get("name", "")).strip()
            if code and code not in seen_codes:
                watch_list.append({"code": code, "name": name, "source": pool_key})
                seen_codes.add(code)

    # 从结构化账本读持仓
    try:
        from scripts.state import load_portfolio_snapshot
        for scope in ("cn_a_system", "paper_mx"):
            snapshot = load_portfolio_snapshot(scope=scope)
            for pos in snapshot.get("positions", []):
                code = str(pos.get("code", "")).strip()
                name = str(pos.get("name", "")).strip()
                if code and code not in seen_codes:
                    watch_list.append({"code": code, "name": name, "source": f"position_{scope}"})
                    seen_codes.add(code)
    except Exception as exc:
        _logger.warning(f"[sentiment] 读取持仓失败: {exc}")

    return watch_list


def _search_news(stock_name: str) -> list[dict]:
    """调用妙想资讯搜索获取最新资讯"""
    try:
        result = dispatch_mx_command("mx.search.news", query=f"{stock_name} 最新资讯")
        if isinstance(result, dict):
            items = result.get("data", result.get("items", result.get("results", [])))
            if isinstance(items, list):
                return items
            # 如果返回的是字符串内容，包装成列表
            content = result.get("content", result.get("text", ""))
            if content:
                return [{"title": stock_name, "content": str(content), "source": "mx_search"}]
        if isinstance(result, str):
            return [{"title": stock_name, "content": result, "source": "mx_search"}]
        return []
    except MXCommandError as exc:
        _logger.info(f"[sentiment] {stock_name} 资讯搜索失败: {exc}")
        return []
    except Exception as exc:
        _logger.warning(f"[sentiment] {stock_name} 资讯搜索异常: {exc}")
        return []


def _match_keywords(text: str) -> list[dict]:
    """匹配关键词，返回命中列表"""
    matches = []
    for level, keywords in NEGATIVE_KEYWORDS.items():
        for kw in keywords:
            if re.search(kw, text):
                matches.append({"keyword": kw, "level": level, "sentiment": "negative"})
    for kw in POSITIVE_KEYWORDS:
        if re.search(kw, text):
            matches.append({"keyword": kw, "level": "info", "sentiment": "positive"})
    return matches


def _load_alert_history() -> dict:
    """加载告警去重历史"""
    try:
        data = load_json_cache("sentiment", CACHE_KEY, max_age_seconds=48 * 3600)
        if isinstance(data, dict) and "data" in data:
            inner = data["data"]
            if isinstance(inner, dict):
                return inner
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {"alerts": {}, "updated_at": ""}


def _save_alert_history(history: dict):
    """保存告警去重历史"""
    history["updated_at"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    try:
        save_json_cache("sentiment", CACHE_KEY, history)
    except Exception as exc:
        _logger.warning(f"[sentiment] 保存告警历史失败: {exc}")


def _is_duplicate(alert_key: str, history: dict) -> bool:
    """检查是否在去重窗口内"""
    last_sent = history.get("alerts", {}).get(alert_key, "")
    if not last_sent:
        return False
    try:
        last_time = datetime.strptime(last_sent, "%Y-%m-%dT%H:%M:%S")
        return (datetime.now() - last_time) < timedelta(hours=DEDUP_HOURS)
    except (ValueError, TypeError):
        return False


def _record_alert(alert_key: str, history: dict):
    """记录告警发送时间"""
    history.setdefault("alerts", {})[alert_key] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def _clean_old_alerts(history: dict):
    """清理超过 48h 的告警记录"""
    cutoff = datetime.now() - timedelta(hours=48)
    alerts = history.get("alerts", {})
    cleaned = {}
    for key, ts in alerts.items():
        try:
            if datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S") > cutoff:
                cleaned[key] = ts
        except (ValueError, TypeError):
            pass
    history["alerts"] = cleaned


def run(dry_run: bool = False) -> dict:
    """
    执行舆情扫描

    Returns:
        {
            "status": "ok" | "warning" | "error",
            "scanned_count": int,
            "alert_count": int,
            "alerts": [...],
            "skipped_duplicate": int,
            "discord_sent": int,
            "discord_failed": int,
        }
    """
    started_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    watch_stocks = _get_watch_stocks()
    if not watch_stocks:
        return {
            "status": "ok",
            "started_at": started_at,
            "scanned_count": 0,
            "alert_count": 0,
            "alerts": [],
            "skipped_duplicate": 0,
            "discord_sent": 0,
            "discord_failed": 0,
            "message": "no_stocks_to_monitor",
        }

    history = _load_alert_history()
    _clean_old_alerts(history)

    alerts = []
    skipped_duplicate = 0
    discord_sent = 0
    discord_failed = 0

    for stock in watch_stocks:
        code = stock["code"]
        name = stock["name"]
        source = stock["source"]

        _logger.info(f"[sentiment] 扫描 {name}({code}) ...")
        news_items = _search_news(name)

        for item in news_items:
            title = str(item.get("title", "")).strip()
            content = str(item.get("content", item.get("summary", ""))).strip()
            full_text = f"{title} {content}"

            matches = _match_keywords(full_text)
            if not matches:
                continue

            # 只对 negative 的 high/warning 级别告警
            negative_matches = [m for m in matches if m["sentiment"] == "negative"]
            if not negative_matches:
                continue

            # 取最高级别
            highest_level = "warning"
            if any(m["level"] == "high" for m in negative_matches):
                highest_level = "high"

            matched_keywords = list({m["keyword"] for m in negative_matches})
            alert_key = f"{code}:{':'.join(sorted(matched_keywords[:3]))}"

            if _is_duplicate(alert_key, history):
                skipped_duplicate += 1
                continue

            alert = {
                "code": code,
                "name": name,
                "source": source,
                "level": highest_level,
                "title": title[:100],
                "summary": content[:200] if content else "",
                "matched_keywords": matched_keywords,
                "sentiment": "negative",
                "url": str(item.get("url", item.get("link", ""))).strip(),
                "alert_key": alert_key,
                "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            }
            alerts.append(alert)

            # Discord 推送
            if not dry_run:
                _record_alert(alert_key, history)
                ok, err = send_sentiment_alert({
                    "matched_keywords": matched_keywords,
                    "source": f"{name}({code})",
                    "title": title[:100],
                    "summary": content[:200] if content else "",
                    "url": alert.get("url", ""),
                    "sentiment": "negative",
                })
                if ok:
                    discord_sent += 1
                else:
                    discord_failed += 1
                    _logger.warning(f"[sentiment] Discord 推送失败: {err}")

    if not dry_run:
        _save_alert_history(history)

    # 写入告警中心
    if alerts and not dry_run:
        try:
            from scripts.state import save_alert_snapshot
            save_alert_snapshot({
                "source": "sentiment_monitor",
                "alerts": alerts,
                "scanned_at": started_at,
            })
        except Exception as exc:
            _logger.warning(f"[sentiment] 告警中心写入失败: {exc}")

    status = "ok"
    if discord_failed > 0:
        status = "warning"

    return {
        "status": status,
        "started_at": started_at,
        "finished_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "scanned_count": len(watch_stocks),
        "alert_count": len(alerts),
        "alerts": alerts,
        "skipped_duplicate": skipped_duplicate,
        "discord_sent": discord_sent,
        "discord_failed": discord_failed,
        "dry_run": dry_run,
    }


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    dry = "--dry-run" in sys.argv
    result = run(dry_run=dry)
    print(json.dumps(result, ensure_ascii=False, indent=2))
