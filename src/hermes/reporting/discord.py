"""
reporting/discord.py — Discord 消息格式化

只负责格式化，不负责发送。实际发送由 Agent Gateway 或 V1 discord_push 处理。
reporting 不反写任何业务表。
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional


# Discord 品牌色
COLORS = {
    "morning": 0x1E88E5,
    "noon": 0xFB8C00,
    "evening": 0x7B1FA2,
    "scoring": 0x00838F,
    "weekly": 0x00695C,
    "stop_alert": 0xC62828,
    "profit_alert": 0x2E7D32,
    "info": 0x37474F,
}

SIGNAL_EMOJI = {"GREEN": "🟢", "YELLOW": "🟡", "RED": "🔴", "CLEAR": "⚪"}
SIGNAL_CN = {"GREEN": "偏强", "YELLOW": "震荡", "RED": "转弱", "CLEAR": "观望"}


def _field(name: str, value: str, inline: bool = True) -> dict:
    return {"name": name[:256], "value": str(value)[:1024], "inline": inline}


def _embed(
    title: str, color: int, fields: list[dict],
    description: str = "", footer: str = "",
) -> dict:
    e: dict = {"title": title[:256], "color": color}
    if description:
        e["description"] = description[:4096]
    if fields:
        e["fields"] = fields
    if footer:
        e["footer"] = {"text": footer}
    e["timestamp"] = datetime.now().astimezone().isoformat()
    return e


def _score_emoji(score: float) -> str:
    if score >= 7:
        return "✅"
    if score >= 5:
        return "🟡"
    return "❌"


def _pnl_emoji(v: float) -> str:
    return "🟢" if v >= 0 else "🔴"


# ---------------------------------------------------------------------------
# 格式化函数
# ---------------------------------------------------------------------------

def format_morning_embed(data: dict) -> dict:
    """盘前摘要 → Discord embed dict。"""
    date_str = data.get("date", datetime.now().strftime("%Y-%m-%d"))
    signal = data.get("market_signal", "")
    sig_tag = f"{SIGNAL_EMOJI.get(signal, '')} {SIGNAL_CN.get(signal, signal)}"

    fields = []

    # 大盘指数
    for name, info in data.get("market", {}).items():
        price = info.get("price", 0)
        chg = info.get("chg_pct", 0)
        fields.append(_field(name, f"`{price:.2f}` ({chg:+.2f}%)"))

    # 持仓
    positions = data.get("positions", [])
    if positions:
        fields.append(_field("\u200b", "**💼 持仓**", inline=False))
        for pos in positions:
            fields.append(_field(
                pos.get("name", ""),
                f"{pos.get('shares', 0)} 股 @ `¥{pos.get('price', 0):.2f}`",
            ))
    else:
        fields.append(_field("💼 持仓", "空仓", inline=False))

    # 核心池
    core = data.get("core_pool", [])
    if core:
        fields.append(_field("\u200b", "**🎯 核心池**", inline=False))
        for s in core:
            score = s.get("score", 0)
            fields.append(_field(s.get("name", ""), f"{_score_emoji(score)} **{score:.1f}**"))

    return _embed(
        title=f"📊 盘前摘要 — {date_str}",
        description=f"综合信号 **{sig_tag}**",
        color=COLORS["morning"],
        fields=fields,
        footer="Hermes · morning_brief",
    )


def format_evening_embed(data: dict) -> dict:
    """收盘报告 → Discord embed dict。"""
    date_str = data.get("date", datetime.now().strftime("%Y-%m-%d"))

    fields = []

    for name, info in data.get("market", {}).items():
        price = info.get("price", 0)
        chg = info.get("chg_pct", 0)
        fields.append(_field(name, f"`{price:.2f}` ({chg:+.2f}%)"))

    positions = data.get("positions", [])
    if positions:
        fields.append(_field("\u200b", "**💼 持仓**", inline=False))
        for pos in positions:
            pnl = pos.get("pnl_pct", 0)
            fields.append(_field(
                f"{_pnl_emoji(pnl)} {pos.get('name', '')}",
                f"{pos.get('shares', 0)} 股 · 盈亏 **{pnl:+.2f}%**",
            ))

    alerts = data.get("alerts", [])
    if alerts:
        fields.append(_field(
            f"⚠️ 触发事项（{len(alerts)}）",
            "\n".join(f"• {a}" for a in alerts),
            inline=False,
        ))

    return _embed(
        title=f"📈 收盘报告 — {date_str}",
        color=COLORS["evening"],
        fields=fields,
        footer="Hermes · close_review",
    )


def format_scoring_embed(scores: list[dict], date_str: str = "") -> dict:
    """评分报告 → Discord embed dict。"""
    if not date_str:
        date_str = datetime.now().strftime("%Y-%m-%d")

    fields = []
    for s in scores[:15]:  # Discord 最多 25 fields
        score = float(s.get("total_score", s.get("total", 0)) or 0)
        name = s.get("name", s.get("code", ""))
        veto = s.get("veto_triggered", False)
        emoji = "❌" if veto else _score_emoji(score)
        detail = f"技{s.get('technical_score', 0):.0f} 基{s.get('fundamental_score', 0):.0f} " \
                 f"资{s.get('flow_score', 0):.0f} 舆{s.get('sentiment_score', 0):.0f}"
        fields.append(_field(f"{emoji} {name}", f"**{score:.1f}** · {detail}"))

    return _embed(
        title=f"🎯 核心池评分 — {date_str}",
        description=f"共 {len(scores)} 只",
        color=COLORS["scoring"],
        fields=fields,
        footer="Hermes · scoring",
    )


def format_stop_alert_embed(signal: dict) -> dict:
    """止损/止盈告警 → Discord embed dict。"""
    code = signal.get("code", "")
    signal_type = signal.get("signal_type", "")
    desc = signal.get("description", "")
    urgency = signal.get("urgency", "")

    color = COLORS["stop_alert"] if "stop" in signal_type else COLORS["profit_alert"]
    title_map = {
        "stop_loss": "🔴 止损触发",
        "trailing_stop": "🟠 移动止盈触发",
        "time_stop": "⏰ 时间止损",
        "ma_exit": "📉 MA 跌破离场",
    }
    title = title_map.get(signal_type, f"⚠️ {signal_type}")

    fields = [
        _field("代码", code),
        _field("类型", signal_type),
        _field("紧急度", urgency),
        _field("说明", desc, inline=False),
    ]

    return _embed(title=title, color=color, fields=fields, footer="Hermes · risk_alert")


def format_weekly_embed(data: dict) -> dict:
    """周报 → Discord embed dict。"""
    week = data.get("week", "")
    buy_count = data.get("buy_count", 0)
    sell_count = data.get("sell_count", 0)
    win_rate = data.get("win_rate", 0)
    profit_loss_ratio = data.get("profit_loss_ratio", 0)
    net_pnl = data.get("net_pnl_cents", 0) / 100
    positions = data.get("positions", [])

    pnl_emoji = "🟢" if net_pnl >= 0 else "🔴"
    sign = "+" if net_pnl >= 0 else ""

    fields = [
        _field("本周收益", f"{pnl_emoji} **{sign}¥{net_pnl:,.0f}**", inline=False),
        _field("买入", f"`{buy_count} 笔`"),
        _field("卖出", f"`{sell_count} 笔`"),
        _field("胜率", f"`{win_rate:.0%}`"),
        _field("盈亏比", f"`{profit_loss_ratio:.2f}`"),
    ]

    if positions:
        pos_lines = []
        for p in positions:
            pos_lines.append(f"• {p.get('name', '')}({p.get('code', '')}) {p.get('shares', 0)}股")
        fields.append(_field("当前持仓", "\n".join(pos_lines) or "空仓", inline=False))
    else:
        fields.append(_field("当前持仓", "空仓", inline=False))

    return _embed(
        title=f"📋 周报 — {week}",
        color=COLORS["weekly"],
        fields=fields,
        footer="Hermes · weekly_review",
    )
