#!/usr/bin/env python3
"""
Discord 格式化推送模块

从 config/notification.yaml 读取 webhook_url，
按 ARCHITECTURE.md §九的模板格式推送消息。

环境变量（优先级高于 YAML）:
  DISCORD_WEBHOOK_URL - Discord webhook URL
"""

import json
import os
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Tuple

try:
    import yaml
except ImportError:
    yaml = None  # PyYAML optional; notification.yaml features disabled

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------

def _get_project_root() -> Path:
    """返回项目根目录（scripts 的上级）"""
    return Path(__file__).resolve().parent.parent.parent


def _load_webhook_url() -> str:
    """
    优先读取环境变量 DISCORD_WEBHOOK_URL，
    否则从 config/notification.yaml 的 discord.webhook_url 读取。
    """
    url = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()
    if url:
        return url

    yaml_path = _get_project_root() / "config" / "notification.yaml"
    if yaml_path.exists() and yaml is not None:
        try:
            with open(yaml_path, encoding="utf-8") as f:
                data = yaml.safe_load(f)
            return (data.get("discord", {}) or {}).get("webhook_url", "").strip()
        except Exception:
            pass
    return ""


# ---------------------------------------------------------------------------
# 底层推送
# ---------------------------------------------------------------------------

def _post_to_discord(content: str) -> Tuple[bool, str]:
    """
    将 content 作为 Discord 消息体 POST 到 webhook。
    content 超过 2000 字符时自动截断（Discord 限制）。

    Returns:
        (success, error_msg)
    """
    url = _load_webhook_url()
    if not url:
        return False, "Discord webhook URL is not configured"

    # Discord 消息长度限制
    if len(content) > 2000:
        content = content[:1997] + "..."

    payload = json.dumps({"content": content}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "AStockTradingBot/1.0",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            if resp.status in (200, 204):
                return True, ""
            return False, f"HTTP {resp.status}"
    except urllib.error.HTTPError as e:
        return False, f"HTTP {e.code}: {e.read().decode()[:200]}"
    except Exception as e:
        return False, str(e)


# ---------------------------------------------------------------------------
# 辅助函数
# ---------------------------------------------------------------------------

def _market_signal_emoji(signal: str) -> str:
    """大盘信号 → emoji"""
    return {"GREEN": "🟢", "YELLOW": "🟡", "RED": "🔴", "CLEAR": "⚪"}.get(signal, signal)


def _score_emoji(score: float) -> str:
    """评分 → emoji（≥7 ✅ / ≥5 🟡 / <5 ❌）"""
    if score >= 7:
        return "✅"
    if score >= 5:
        return "🟡"
    return "❌"


def _fmt_pct(v: float) -> str:
    """浮点数 → 带符号百分比字符串，如 +0.15%"""
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.2f}%"


def _fmt_price(code: str, price: float, currency: str = "¥") -> str:
    """格式化价格"""
    return f"{currency}{price:.2f}"


# ---------------------------------------------------------------------------
# 模板函数
# ---------------------------------------------------------------------------

def _build_morning_summary(data: dict) -> str:
    """盘前摘要模板"""
    date_str = data.get("date", datetime.now().strftime("%Y-%m-%d"))
    weekday = data.get("weekday", "")
    if weekday:
        date_str = f"{date_str}（{weekday}）"

    lines = [
        f"📊 盘前摘要 — {date_str}",
        "",
        "━━━━━━━━━━━━━━━━━━━━",
        "🟢 大盘",
        "━━━━━━━━━━━━━━━━━━━━",
    ]

    for name, info in data.get("market", {}).items():
        price = info.get("price", 0)
        chg = info.get("chg_pct", 0)
        ma20 = info.get("ma20_pct", 0)
        ma60 = info.get("ma60_pct", 0)
        signal = info.get("signal", "")

        lines.append(f"  {name}: {price:.2f} ({_fmt_pct(chg)})")
        ma20_ok = "✅" if ma20 >= 0 else "🔴"
        lines.append(f"     vs MA20: {_fmt_pct(ma20)} {ma20_ok}")
        ma60_days = info.get("ma60_days", 0)
        ma60_ok = "✅" if ma60 >= 0 else "🔴"
        if ma60_days:
            lines.append(f"     vs MA60: {_fmt_pct(ma60)} {ma60_ok}（{ma60_days}日）")
        else:
            lines.append(f"     vs MA60: {_fmt_pct(ma60)} {ma60_ok}")

    signal = data.get("market_signal", "")
    if signal:
        lines.append(f"  🔔 {signal}")

    lines.extend(["", "━━━━━━━━━━━━━━━━━━━━", "💼 持仓", "━━━━━━━━━━━━━━━━━━━━"])
    for pos in data.get("positions", []):
        name = pos.get("name", "")
        shares = pos.get("shares", 0)
        price = pos.get("price", 0)
        currency = pos.get("currency", "¥")
        note = pos.get("note", "")
        if name:
            lines.append(f"  {name} {shares}股 @ {currency}{price:.2f}")
            if note:
                lines.append(f"  {note}")
    if not data.get("positions"):
        lines.append("  （暂无持仓）")

    lines.extend(["", "━━━━━━━━━━━━━━━━━━━━", "🎯 核心池", "━━━━━━━━━━━━━━━━━━━━"])
    for stock in data.get("core_pool", []):
        name = stock.get("name", "")
        score = stock.get("score", 0)
        note = stock.get("note", "")
        lines.append(f"  {name} {score:.1f} {_score_emoji(score)} | {note}")

    lines.extend(["", "━━━━━━━━━━━━━━━━━━━━", "📋 今日计划", "━━━━━━━━━━━━━━━━━━━━"])
    weekly_bought = data.get("weekly_bought", 0)
    weekly_limit = data.get("weekly_limit", 2)
    if weekly_bought < weekly_limit:
        lines.append(f"  本周买入: {weekly_bought}/{weekly_limit} | 可正常买入")
    else:
        lines.append(f"  本周买入: {weekly_bought}/{weekly_limit} | ⚠️ 本周额度已用完")

    return "\n".join(lines)


def _build_noon_check(data: dict) -> str:
    """午休检查模板"""
    date_str = data.get("date", datetime.now().strftime("%Y-%m-%d"))
    weekday = data.get("weekday", "")
    if weekday:
        date_str = f"{date_str}（{weekday}）"

    lines = [
        f"☀️ 午休检查 — {date_str}",
        "",
        "━━━━━━━━━━━━━━━━━━━━",
        "📈 上午行情",
        "━━━━━━━━━━━━━━━━━━━━",
    ]

    for name, info in data.get("market", {}).items():
        price = info.get("price", 0)
        chg = info.get("chg_pct", 0)
        high = info.get("high", 0)
        low = info.get("low", 0)
        lines.append(f"  {name}: {price:.2f} ({_fmt_pct(chg)})")
        if high and low:
            lines.append(f"    区间: {low:.2f} ~ {high:.2f}")

    lines.extend(["", "━━━━━━━━━━━━━━━━━━━━", "💼 持仓状态", "━━━━━━━━━━━━━━━━━━━━"])
    for pos in data.get("positions", []):
        name = pos.get("name", "")
        shares = pos.get("shares", 0)
        cost = pos.get("cost", 0)
        price = pos.get("price", 0)
        pnl = pos.get("pnl_pct", 0)
        currency = pos.get("currency", "¥")
        lines.append(f"  {name} {shares}股")
        lines.append(f"    成本: {currency}{cost:.2f} | 现价: {currency}{price:.2f} | 盈亏: {_fmt_pct(pnl)}")

    lines.extend(["", "━━━━━━━━━━━━━━━━━━━━", "📋 午休提示", "━━━━━━━━━━━━━━━━━━━━"])
    tips = data.get("tips", [])
    if tips:
        for t in tips:
            lines.append(f"  • {t}")
    else:
        lines.append("  （无特殊提示）")

    return "\n".join(lines)


def _build_evening_report(data: dict) -> str:
    """收盘报告模板"""
    date_str = data.get("date", datetime.now().strftime("%Y-%m-%d"))
    weekday = data.get("weekday", "")
    if weekday:
        date_str = f"{date_str}（{weekday}）"

    lines = [
        f"📈 收盘报告 — {date_str}",
        "",
        "━━━━━━━━━━━━━━━━━━━━",
        "📊 大盘",
        "━━━━━━━━━━━━━━━━━━━━",
    ]

    for name, info in data.get("market", {}).items():
        price = info.get("price", 0)
        chg = info.get("chg_pct", 0)
        signal = info.get("signal", "")
        signal_mark = f"🔔 {signal}" if signal else ""
        lines.append(f"  {name}: {price:.2f} ({_fmt_pct(chg)}) {signal_mark}")

    lines.extend(["", "━━━━━━━━━━━━━━━━━━━━", "💰 持仓", "━━━━━━━━━━━━━━━━━━━━"])
    for pos in data.get("positions", []):
        name = pos.get("name", "")
        shares = pos.get("shares", 0)
        value = pos.get("value", 0)
        currency = pos.get("currency", "¥")
        status = pos.get("status", "持仓中")
        lines.append(f"  {name}: {currency}{value:.0f}（{status}）")
    total_value = data.get("total_value", 0)
    if total_value:
        lines.append(f"  账户总值: ~{data.get('currency', '¥')}{total_value:.0f}")
    if not data.get("positions"):
        lines.append("  A股: 空仓")

    lines.extend(["", "━━━━━━━━━━━━━━━━━━━━", "⚠️ 触发事项", "━━━━━━━━━━━━━━━━━━━━"])
    alerts = data.get("alerts", [])
    if alerts:
        for a in alerts:
            lines.append(f"  • {a}")
    else:
        lines.append("  无")

    lines.extend(["", "━━━━━━━━━━━━━━━━━━━━", "🎯 核心池今日评分", "━━━━━━━━━━━━━━━━━━━━"])
    for stock in data.get("core_pool", []):
        name = stock.get("name", "")
        score = stock.get("score", 0)
        note = stock.get("note", "")
        lines.append(f"  {name} {score:.1f} {_score_emoji(score)} | {note}")

    lines.extend(["", "━━━━━━━━━━━━━━━━━━━━", "📋 明日计划", "━━━━━━━━━━━━━━━━━━━━"])
    plan = data.get("tomorrow_plan", [])
    if plan:
        for item in plan:
            lines.append(f"  • {item}")
    else:
        lines.append("  （暂无计划）")

    return "\n".join(lines)


def _build_weekly_report(data: dict) -> str:
    """周报模板"""
    week_str = data.get("week", datetime.now().strftime("%Y-W%W"))
    year_str = data.get("year", datetime.now().strftime("%Y"))

    lines = [
        f"📊 周报 — {year_str} {week_str}",
        "",
        "━━━━━━━━━━━━━━━━━━━━",
        "📈 本周收益",
        "━━━━━━━━━━━━━━━━━━━━",
    ]

    pnl_pct = data.get("pnl_pct", 0)
    pnl_abs = data.get("pnl_abs", 0)
    lines.append(f"  本周 P&L: {_fmt_pct(pnl_pct)}（{data.get('currency', '¥')}{pnl_abs:+.2f}）")

    win_rate = data.get("win_rate", 0)
    trades = data.get("trades", 0)
    profit_loss_ratio = data.get("profit_loss_ratio", 0)
    lines.append(f"  胜率: {win_rate:.0%} | 交易次数: {trades} | 盈亏比: {profit_loss_ratio:.2f}")

    lines.extend(["", "━━━━━━━━━━━━━━━━━━━━", "💼 持仓变化", "━━━━━━━━━━━━━━━━━━━━"])
    for entry in data.get("position_changes", []):
        action = entry.get("action", "")  # buy/sell/hold
        name = entry.get("name", "")
        shares = entry.get("shares", 0)
        price = entry.get("price", 0)
        currency = entry.get("currency", "¥")
        emoji = {"buy": "🟢买入", "sell": "🔴卖出", "hold": "⚪持有"}.get(action, action)
        lines.append(f"  {emoji} {name} {shares}股 @ {currency}{price:.2f}")

    lines.extend(["", "━━━━━━━━━━━━━━━━━━━━", "🎯 核心池异动", "━━━━━━━━━━━━━━━━━━━━"])
    for stock in data.get("core_pool_changes", []):
        name = stock.get("name", "")
        old_score = stock.get("old_score", 0)
        new_score = stock.get("new_score", 0)
        reason = stock.get("reason", "")
        chg = new_score - old_score
        chg_mark = f"{'+' if chg >= 0 else ''}{chg:.1f}"
        lines.append(f"  {name}: {old_score:.1f} → {new_score:.1f} ({chg_mark}) | {reason}")

    lines.extend(["", "━━━━━━━━━━━━━━━━━━━━", "📋 下周计划", "━━━━━━━━━━━━━━━━━━━━"])
    for item in data.get("next_week_plan", []):
        lines.append(f"  • {item}")

    return "\n".join(lines)


def _build_sentiment_alert(data: dict) -> str:
    """舆情提醒模板"""
    keywords = data.get("matched_keywords", [])
    keyword_str = " / ".join(keywords) if keywords else "（关键词）"

    lines = [
        f"🔔 舆情提醒 — {keyword_str}",
        "",
        "━━━━━━━━━━━━━━━━━━━━",
        f"📰 来源: {data.get('source', '未知')}",
        "━━━━━━━━━━━━━━━━━━━━",
    ]

    title = data.get("title", "（无标题）")
    lines.append(f"**{title}**")

    summary = data.get("summary", "")
    if summary:
        lines.append(f"\n{summary}")

    url = data.get("url", "")
    if url:
        lines.append(f"\n🔗 {url}")

    sentiment = data.get("sentiment", "")  # positive / negative / neutral
    sentiment_mark = {
        "positive": "🟢 正面",
        "negative": "🔴 负面",
        "neutral": "⚪ 中性",
    }.get(sentiment, sentiment)
    if sentiment_mark:
        lines.append(f"情绪: {sentiment_mark}")

    return "\n".join(lines)


def _build_condition_order_reminder(pending: list) -> str:
    """条件单待确认提醒模板"""
    lines = [
        "🔔 条件单待确认提醒",
        "",
        "━━━━━━━━━━━━━━━━━━━━",
        "以下条件单需要您确认：",
        "",
    ]

    if not pending:
        lines.append("（无待确认条件单）")
    else:
        for item in pending:
            name = item.get("name", "")
            order_type = item.get("type", "条件单")  # 止损 / 止盈
            price = item.get("price", 0)
            currency = item.get("currency", "¥")
            status = item.get("status", "待确认")
            lines.append(f"  📌 {name}")
            lines.append(f"     {order_type} @ {currency}{price:.2f} — {status}")

    lines.extend([
        "",
        "请回复确认：",
        "• \"止损触发了 {股票名} 成交¥{价格}\" 或 \"取消止损 {股票名}\"",
        "• \"止盈触发了 {股票名} 成交¥{价格}\" 或 \"取消止盈 {股票名}\"",
    ])
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 公开接口
# ---------------------------------------------------------------------------

def send_morning_summary(data: dict) -> Tuple[bool, str]:
    """
    盘前摘要（8:25）

    data 字段：
      date, weekday, market_signal
      market: {名称: {price, chg_pct, ma20_pct, ma60_pct, ma60_days, signal}}
      positions: [{name, shares, price, currency, note}]
      core_pool: [{name, score, note}]
      weekly_bought, weekly_limit
    """
    content = _build_morning_summary(data)
    return _post_to_discord(content)


def send_noon_check(data: dict) -> Tuple[bool, str]:
    """
    午休检查（11:55）

    data 字段：
      date, weekday
      market: {名称: {price, chg_pct, high, low}}
      positions: [{name, shares, cost, price, pnl_pct, currency}]
      tips: [str, ...]
    """
    content = _build_noon_check(data)
    return _post_to_discord(content)


def send_evening_report(data: dict) -> Tuple[bool, str]:
    """
    收盘报告（15:35）

    data 字段：
      date, weekday, currency
      market: {名称: {price, chg_pct, signal}}
      positions: [{name, shares, value, currency, status}]
      total_value
      alerts: [str, ...]
      core_pool: [{name, score, note}]
      tomorrow_plan: [str, ...]
    """
    content = _build_evening_report(data)
    return _post_to_discord(content)


def send_weekly_report(data: dict) -> Tuple[bool, str]:
    """
    周报（周日20:00）

    data 字段：
      year, week, currency
      pnl_pct, pnl_abs
      win_rate, trades, profit_loss_ratio
      position_changes: [{action, name, shares, price, currency}]
      core_pool_changes: [{name, old_score, new_score, reason}]
      next_week_plan: [str, ...]
    """
    content = _build_weekly_report(data)
    return _post_to_discord(content)


def send_sentiment_alert(data: dict) -> Tuple[bool, str]:
    """
    舆情提醒

    data 字段：
      matched_keywords: [str, ...]
      source, title, summary, url
      sentiment: positive / negative / neutral
    """
    content = _build_sentiment_alert(data)
    return _post_to_discord(content)


def send_condition_order_reminder(pending: list) -> Tuple[bool, str]:
    """
    条件单待确认提醒

    pending 元素：
      {name, type, price, currency, status}
    """
    content = _build_condition_order_reminder(pending)
    return _post_to_discord(content)
