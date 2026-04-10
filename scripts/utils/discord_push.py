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


# ---------------------------------------------------------------------------------------------------------------------------------------------
# 底层推送
# ---------------------------------------------------------------------------------------------------------------------------------------------

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


# Discord 品牌色（整数 RGB）
DISCORD_COLORS = {
    "morning":   0x1E88E5,  # 蓝色 — 盘前活力
    "noon":      0xFB8C00,  # 橙色 — 午间暖阳
    "evening":   0x7B1FA2,  # 紫色 — 收盘沉稳
    "weekly":    0x00695C,  # 深青 — 周报专业
    "sentiment": 0xC62828,  # 红色 — 舆情警报
    "hk_alert":  0x880E4F,  # 深红 — 港股告警
    "hk_summary":0x4A148C,  # 紫黑 — 港股汇总
    "info":      0x37474F,  # 灰蓝 — 通用信息
}


def _post_embed_to_discord(
    embeds: list[dict],
    content: str = "",
    username: str = "Hermes 交易系统",
    avatar_url: str = "",
) -> Tuple[bool, str]:
    """
    将 Discord 原生 Rich Embed POST 到 webhook。

    Args:
        embeds: Embed 对象列表（每个即一张卡片），Discord 单次最多 10 条。
        content: 普通文本内容（@ 用户等）。
        username / avatar_url: 自定义机器人名字/头像。

    Returns:
        (success, error_msg)
    """
    url = _load_webhook_url()
    if not url:
        return False, "Discord webhook URL is not configured"

    # 截断 content
    if len(content) > 2000:
        content = content[:1997] + "..."

    payload = {
        "content": content,
        "embeds": embeds[:10],  # Discord 上限 10 条
    }
    if username:
        payload["username"] = username
    if avatar_url:
        payload["avatar_url"] = avatar_url

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
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


# ---------------------------------------------------------------------------------------------------------------------------------------------
# Embed Builder 工具
# ---------------------------------------------------------------------------------------------------------------------------------------------

def _footer(text: str, ts: str = "") -> dict:
    """标准 footer（可带时间戳）。"""
    icon = f" · {ts}" if ts else ""
    return {"text": f"{text}{icon}"}


def _field(name: str, value: str, inline: bool = True) -> dict:
    """标准字段。"""
    # Discord field value 最多 1024 字符
    return {"name": name, "value": str(value)[:1024], "inline": inline}


def _compact_field(name: str, value: str) -> dict:
    """一行两个字段（inline=True）。"""
    return _field(name, str(value)[:1024], inline=True)


def _build_embed(
    title: str,
    description: str = "",
    color: int = 0x37474F,
    fields: list[dict] | None = None,
    footer: dict | None = None,
    thumbnail_url: str = "",
    image_url: str = "",
    author_name: str = "",
    author_icon: str = "",
    url: str = "",
) -> dict:
    """组装一条 Discord Embed。"""
    embed = {"title": title[:256], "color": color}
    if description:
        embed["description"] = description[:4096]
    if url:
        embed["url"] = url
    if fields:
        embed["fields"] = [_field(**f) if isinstance(f, dict) else f for f in fields]
    if footer:
        embed["footer"] = footer
    if thumbnail_url:
        embed["thumbnail"] = {"url": thumbnail_url}
    if image_url:
        embed["image"] = {"url": image_url}
    if author_name:
        embed["author"] = {"name": author_name, "icon_url": author_icon} if author_icon else {"name": author_name}
    return embed


# ---------------------------------------------------------------------------
# 辅助函数
# ---------------------------------------------------------------------------

def _market_signal_emoji(signal: str) -> str:
    """大盘信号 → emoji"""
    return {"GREEN": "🟢", "YELLOW": "🟡", "RED": "🔴", "CLEAR": "⚪"}.get(signal, signal)


# 大盘信号中文映射（面向用户展示）
SIGNAL_CN = {
    "GREEN": "偏强",
    "YELLOW": "震荡",
    "RED": "转弱",
    "CLEAR": "观望",
}

SIGNAL_EMOJI_CN = {
    "GREEN": "🟢 偏强",
    "YELLOW": "🟡 震荡",
    "RED": "🔴 转弱",
    "CLEAR": "⚪ 观望",
}


def _signal_cn(signal: str) -> str:
    """大盘信号 → 中文标签"""
    return SIGNAL_CN.get(signal, signal)


def _signal_emoji_cn(signal: str) -> str:
    """大盘信号 → emoji + 中文标签"""
    return SIGNAL_EMOJI_CN.get(signal, signal)


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
# Embed Builder — 盘前摘要
# ---------------------------------------------------------------------------

def _build_morning_embeds(data: dict) -> list[dict]:
    """
    盘前摘要 → 多卡片 embed。
    第一张：大盘信号
    第二张：持仓（如有）
    第三张：核心池
    """
    date_str = data.get("date", datetime.now().strftime("%Y-%m-%d"))
    weekday = data.get("weekday", "")
    if weekday:
        date_str = f"{date_str}（{weekday}）"
    ts = datetime.now().strftime("%H:%M")

    embeds = []

    # ── 大盘信号卡片 ──────────────────────────────────────────────
    market_fields = []
    signal_emoji_map = {"GREEN": "🟢", "YELLOW": "🟡", "RED": "🔴", "CLEAR": "⚪"}
    signal_cn_map   = {"GREEN": "偏强", "YELLOW": "震荡", "RED": "转弱", "CLEAR": "观望"}

    for name, info in data.get("market", {}).items():
        price  = info.get("price", 0)
        chg    = info.get("chg_pct", 0)
        ma20   = info.get("ma20_pct", 0)
        ma60   = info.get("ma60_pct", 0)
        sig    = info.get("signal", "")
        ma20_ok = "✅" if ma20 >= 0 else "🔴"
        ma60_ok = "✅" if ma60 >= 0 else "🔴"
        sign = "+" if chg >= 0 else ""
        sig_emoji = signal_emoji_map.get(sig, "")
        sig_cn    = signal_cn_map.get(sig, sig)

        field_lines = [
            f"**现价** `{price:.2f}`（{sign}{chg:.2f}%）",
            f"vs MA20：{sign}{ma20:.2f}% {ma20_ok}",
            f"vs MA60：{sign}{ma60:.2f}% {ma60_ok}",
        ]
        market_fields.append({
            "name": f"{sig_emoji} {name} {sig_cn}",
            "value": "\n".join(field_lines),
            "inline": True,
        })

    market_signal = data.get("market_signal", "")
    signal_tag = f"{signal_emoji_map.get(market_signal, '')} {signal_cn_map.get(market_signal, market_signal)}"

    embeds.append(_build_embed(
        title=f"📊 盘前摘要 — {date_str}",
        description=f"大盘信号 **{signal_tag}**",
        color=DISCORD_COLORS["morning"],
        fields=market_fields,
        footer=_footer("Hermes Cron · morning_brief", ts),
        author_name="Hermes 交易系统",
    ))

    # ── 持仓卡片 ──────────────────────────────────────────────────
    positions = data.get("positions", [])
    if positions:
        pos_fields = []
        for pos in positions:
            name  = pos.get("name", "")
            shares = pos.get("shares", 0)
            price = pos.get("price", 0)
            currency = pos.get("currency", "¥")
            note = pos.get("note", "")
            lines = [f"**{shares}** 股 @ `{currency}{price:.2f}`"]
            if note:
                lines.append(f"_{note}_")
            pos_fields.append({
                "name": name,
                "value": "\n".join(lines),
                "inline": True,
            })
        embeds.append(_build_embed(
            title="💼 当前持仓",
            color=DISCORD_COLORS["morning"],
            fields=pos_fields,
            footer=_footer(f"{len(positions)} 只", ts),
        ))
    else:
        embeds.append(_build_embed(
            title="💼 当前持仓",
            description="（暂无持仓）",
            color=DISCORD_COLORS["morning"],
            footer=_footer("空仓", ts),
        ))

    # ── 核心池卡片 ────────────────────────────────────────────────
    core = data.get("core_pool", [])
    if core:
        core_fields = []
        for stock in core:
            name  = stock.get("name", "")
            score = stock.get("score", 0)
            note  = stock.get("note", "")
            emoji = "✅" if score >= 7 else "🟡" if score >= 5 else "❌"
            lines = [f"**{emoji} {score:.1f}**"]
            if note:
                lines.append(f"_{note}_")
            core_fields.append({
                "name": name,
                "value": "\n".join(lines),
                "inline": True,
            })
        embeds.append(_build_embed(
            title="🎯 核心池",
            color=DISCORD_COLORS["morning"],
            fields=core_fields,
            footer=_footer(f"{len(core)} 只候选", ts),
        ))

    # ── 交易计划卡片 ──────────────────────────────────────────────
    wb  = data.get("weekly_bought", 0)
    wl  = data.get("weekly_limit", 2)
    rem = wl - wb
    plan_ok = rem > 0
    plan_fields = [
        {"name": "本周买入", "value": f"`{wb}/{wl}`", "inline": True},
        {"name": "本周剩余", "value": f"`{rem} 次` {'✅ 可正常买入' if plan_ok else '⚠️ 额度用完'}", "inline": True},
    ]
    embeds.append(_build_embed(
        title="📋 今日交易计划",
        color=DISCORD_COLORS["morning"],
        fields=plan_fields,
        footer=_footer("Hermes Cron · morning_brief", ts),
    ))

    return embeds


# ---------------------------------------------------------------------------
# Embed Builder — 午休检查
# ---------------------------------------------------------------------------

def _build_noon_embeds(data: dict) -> list[dict]:
    date_str = data.get("date", datetime.now().strftime("%Y-%m-%d"))
    weekday  = data.get("weekday", "")
    if weekday:
        date_str = f"{date_str}（{weekday}）"
    ts = datetime.now().strftime("%H:%M")

    embeds = []

    # 大盘
    market_fields = []
    for name, info in data.get("market", {}).items():
        price = info.get("price", 0)
        chg   = info.get("chg_pct", 0)
        high  = info.get("high", 0)
        low   = info.get("low", 0)
        sign  = "+" if chg >= 0 else ""
        lines = [f"**现价** `{price:.2f}`（{sign}{chg:.2f}%）"]
        if high and low:
            lines.append(f"区间：`{low:.2f}` ~ `{high:.2f}`")
        market_fields.append({
            "name": name,
            "value": "\n".join(lines),
            "inline": True,
        })

    embeds.append(_build_embed(
        title=f"☀️ 午休检查 — {date_str}",
        color=DISCORD_COLORS["noon"],
        fields=market_fields,
        footer=_footer("Hermes Cron · noon_check", ts),
        author_name="Hermes 交易系统",
    ))

    # 持仓
    positions = data.get("positions", [])
    pos_fields = []
    for pos in positions:
        name   = pos.get("name", "")
        shares = pos.get("shares", 0)
        cost   = pos.get("cost", 0)
        price  = pos.get("price", 0)
        pnl    = pos.get("pnl_pct", 0)
        cur    = pos.get("currency", "¥")
        sign   = "+" if pnl >= 0 else ""
        emoji  = "🟢" if pnl >= 0 else "🔴"
        pos_fields.append({
            "name": emoji,
            "value": f"**{name}**\n`{shares}` 股\n成本 `{cur}{cost:.2f}`\n现价 `{cur}{price:.2f}`\n盈亏 {sign}{pnl:.2f}%",
            "inline": True,
        })

    if pos_fields:
        embeds.append(_build_embed(
            title="💼 持仓状态",
            color=DISCORD_COLORS["noon"],
            fields=pos_fields,
            footer=_footer(f"{len(positions)} 只", ts),
        ))
    else:
        embeds.append(_build_embed(
            title="💼 持仓状态",
            description="（暂无持仓）",
            color=DISCORD_COLORS["noon"],
            footer=_footer("空仓", ts),
        ))

    # 提示
    tips = data.get("tips", [])
    tips_value = "\n".join(f"• {t}" for t in tips) if tips else "（无特殊提示）"
    embeds.append(_build_embed(
        title="📋 午休提示",
        description=tips_value,
        color=DISCORD_COLORS["noon"],
        footer=_footer("Hermes Cron · noon_check", ts),
    ))

    return embeds


# ---------------------------------------------------------------------------
# Embed Builder — 收盘报告
# ---------------------------------------------------------------------------

def _build_evening_embeds(data: dict) -> list[dict]:
    date_str = data.get("date", datetime.now().strftime("%Y-%m-%d"))
    weekday  = data.get("weekday", "")
    if weekday:
        date_str = f"{date_str}（{weekday}）"
    ts = datetime.now().strftime("%H:%M")

    embeds = []

    # 大盘
    market_fields = []
    signal_cn_map = {"GREEN": "偏强", "YELLOW": "震荡", "RED": "转弱", "CLEAR": "观望"}
    for name, info in data.get("market", {}).items():
        price = info.get("price", 0)
        chg   = info.get("chg_pct", 0)
        sig   = info.get("signal", "")
        sign  = "+" if chg >= 0 else ""
        sig_cn = signal_cn_map.get(sig, sig)
        market_fields.append({
            "name": name,
            "value": f"**`{price:.2f}`**（{sign}{chg:.2f}%）{' 🔔 ' + sig_cn if sig_cn else ''}",
            "inline": True,
        })
    embeds.append(_build_embed(
        title=f"📈 收盘报告 — {date_str}",
        color=DISCORD_COLORS["evening"],
        fields=market_fields,
        footer=_footer("Hermes Cron · close_review", ts),
        author_name="Hermes 交易系统",
    ))

    # 持仓
    positions = data.get("positions", [])
    pos_fields = []
    for pos in positions:
        name   = pos.get("name", "")
        shares = pos.get("shares", 0)
        value  = pos.get("value", 0)
        cur    = pos.get("currency", "¥")
        status = pos.get("status", "持仓中")
        pos_fields.append({
            "name": name,
            "value": f"**{cur}{value:.0f}**\n{status}",
            "inline": True,
        })
    total_value = data.get("total_value", 0)
    account_line = f"账户总值 ~**{data.get('currency','¥')}{total_value:.0f}**" if total_value else ""

    if pos_fields or account_line:
        embeds.append(_build_embed(
            title="💰 持仓",
            description=account_line if account_line and not pos_fields else None,
            color=DISCORD_COLORS["evening"],
            fields=pos_fields if pos_fields else None,
            footer=_footer(f"{len(positions)} 只" if positions else "A股空仓", ts),
        ))

    # 触发事项
    alerts = data.get("alerts", [])
    alerts_value = "\n".join(f"• {a}" for a in alerts) if alerts else "（无）"
    embeds.append(_build_embed(
        title="⚠️ 触发事项",
        description=alerts_value,
        color=DISCORD_COLORS["evening"],
        footer=_footer(f"{len(alerts)} 项" if alerts else "无", ts),
    ))

    # 核心池
    core = data.get("core_pool", [])
    if core:
        core_fields = []
        for stock in core:
            name  = stock.get("name", "")
            score = stock.get("score", 0)
            note  = stock.get("note", "")
            emoji = "✅" if score >= 7 else "🟡" if score >= 5 else "❌"
            core_fields.append({
                "name": f"{emoji} {name}",
                "value": f"**{score:.1f}**\n_{note}_" if note else f"**{score:.1f}**",
                "inline": True,
            })
        embeds.append(_build_embed(
            title="🎯 核心池今日评分",
            color=DISCORD_COLORS["evening"],
            fields=core_fields,
            footer=_footer(f"{len(core)} 只", ts),
        ))

    # 明日计划
    plan = data.get("tomorrow_plan", [])
    plan_value = "\n".join(f"• {item}" for item in plan) if plan else "（暂无计划）"
    embeds.append(_build_embed(
        title="📋 明日计划",
        description=plan_value,
        color=DISCORD_COLORS["evening"],
        footer=_footer("Hermes Cron · close_review", ts),
    ))

    return embeds


# ---------------------------------------------------------------------------
# Embed Builder — 周报
# ---------------------------------------------------------------------------

def _build_weekly_embeds(data: dict) -> list[dict]:
    week_str = data.get("week", datetime.now().strftime("W%W"))
    year_str = data.get("year", datetime.now().strftime("%Y"))
    ts = datetime.now().strftime("%H:%M")

    embeds = []

    # 收益总览
    pnl_pct = data.get("pnl_pct", 0)
    pnl_abs = data.get("pnl_abs", 0)
    sign    = "+" if pnl_pct >= 0 else ""
    emoji   = "🟢" if pnl_pct >= 0 else "🔴"
    pnl_fields = [
        {"name": "本周收益", "value": f"{emoji} **{sign}{pnl_pct:.2f}%**（{data.get('currency','¥')}{pnl_abs:+,.2f}）", "inline": False},
        {"name": "胜率", "value": f"`{data.get('win_rate',0):.0%}`", "inline": True},
        {"name": "交易次数", "value": f"`{data.get('trades',0)}`", "inline": True},
        {"name": "盈亏比", "value": f"`{data.get('profit_loss_ratio',0):.2f}`", "inline": True},
    ]
    embeds.append(_build_embed(
        title=f"📊 周报 — {year_str} {week_str}",
        color=DISCORD_COLORS["weekly"],
        fields=pnl_fields,
        footer=_footer("Hermes Cron · weekly_review", ts),
        author_name="Hermes 交易系统",
    ))

    # 持仓变化
    changes = data.get("position_changes", [])
    if changes:
        change_fields = []
        for entry in changes:
            action = entry.get("action", "hold")
            name   = entry.get("name", "")
            shares = entry.get("shares", 0)
            price  = entry.get("price", 0)
            cur    = entry.get("currency", "¥")
            emap   = {"buy": "🟢 买入", "sell": "🔴 卖出", "hold": "⚪ 持有"}
            change_fields.append({
                "name": emap.get(action, action),
                "value": f"**{name}** `{shares}` 股 @ `{cur}{price:.2f}`",
                "inline": True,
            })
        embeds.append(_build_embed(
            title="💼 持仓变化",
            color=DISCORD_COLORS["weekly"],
            fields=change_fields,
            footer=_footer(f"{len(changes)} 笔", ts),
        ))

    # 核心池异动
    core_changes = data.get("core_pool_changes", [])
    if core_changes:
        cc_fields = []
        for stock in core_changes:
            name      = stock.get("name", "")
            old_score = stock.get("old_score", 0)
            new_score = stock.get("new_score", 0)
            reason    = stock.get("reason", "")
            chg       = new_score - old_score
            sign_chg  = f"+{chg:.1f}" if chg >= 0 else f"{chg:.1f}"
            cc_fields.append({
                "name": name,
                "value": f"`{old_score:.1f}` → `{new_score:.1f}`（{sign_chg}）\n_{reason}_",
                "inline": True,
            })
        embeds.append(_build_embed(
            title="🎯 核心池异动",
            color=DISCORD_COLORS["weekly"],
            fields=cc_fields,
            footer=_footer(f"{len(core_changes)} 只", ts),
        ))

    # 下周计划
    next_plan = data.get("next_week_plan", [])
    plan_value = "\n".join(f"• {item}" for item in next_plan) if next_plan else "（暂无计划）"
    embeds.append(_build_embed(
        title="📋 下周计划",
        description=plan_value,
        color=DISCORD_COLORS["weekly"],
        footer=_footer("Hermes Cron · weekly_review", ts),
    ))

    return embeds


# ---------------------------------------------------------------------------
# Embed Builder — 舆情提醒
# ---------------------------------------------------------------------------

def _build_sentiment_embeds(data: dict) -> list[dict]:
    ts = datetime.now().strftime("%H:%M")
    keywords = data.get("matched_keywords", [])
    kw_str    = " / ".join(keywords) if keywords else ""

    sentiment_map = {
        "positive": ("🟢 正面", 0x2E7D32),
        "negative": ("🔴 负面", 0xC62828),
        "neutral":  ("⚪ 中性", 0x37474F),
    }
    sent_tag, sent_color = sentiment_map.get(data.get("sentiment", "neutral"), ("⚪ 中性", 0x37474F))

    fields = [
        {"name": "关键词", "value": kw_str or "（关键词）", "inline": True},
        {"name": "情绪", "value": sent_tag, "inline": True},
        {"name": "来源", "value": data.get("source", "未知"), "inline": True},
    ]
    url = data.get("url", "")
    if url:
        fields.append({"name": "链接", "value": url[:200], "inline": False})

    embeds = [_build_embed(
        title=f"🔔 舆情提醒 — {kw_str}",
        description=data.get("title", "（无标题）"),
        color=sent_color,
        fields=fields,
        footer=_footer("Hermes Cron · sentiment", ts),
        author_name="Hermes 交易系统",
    )]

    summary = data.get("summary", "")
    if summary:
        embeds.append(_build_embed(
            description=summary[:4096],
            color=sent_color,
            footer=_footer("详情摘要", ts),
        ))

    return embeds


# ---------------------------------------------------------------------------
# Embed Builder — 港股遗留仓位
# ---------------------------------------------------------------------------

def _build_hk_embeds(positions: list[dict], ts: str = "") -> list[dict]:
    """港股监控汇总卡（每只股票一张子卡片）。"""
    if not ts:
        ts = datetime.now().strftime("%H:%M")
    embeds = []

    embeds.append(_build_embed(
        title="🔍 港股遗留仓位监控报告",
        description=f"📅 {datetime.now().strftime('%Y-%m-%d')}",
        color=DISCORD_COLORS["hk_summary"],
        footer=_footer(f"{len(positions)} 只持仓", ts),
        author_name="Hermes Cron · hk_monitor",
    ))

    for pos in positions:
        name   = pos.get("name", "")
        code   = pos.get("code", "")
        shares = pos.get("shares", 0)
        cost   = pos.get("avg_cost", 0)
        cur_price = pos.get("current_price", 0)
        cur_value = cur_price * shares
        pnl    = (cur_price / cost - 1) * 100 if cost > 0 else 0
        stop   = pos.get("stop_loss_price", cost)
        sign   = "+" if pnl >= 0 else ""
        emoji  = "🟢" if pnl >= 0 else "🔴"

        fields = [
            {"name": "持有股数", "value": f"`{shares}` 股", "inline": True},
            {"name": "平均成本", "value": f"`HK${cost:.2f}`", "inline": True},
            {"name": "现价", "value": f"`HK${cur_price:.2f}`", "inline": True},
            {"name": "市值", "value": f"**HK${cur_value:,.0f}**", "inline": True},
            {"name": f"浮亏 {emoji}", "value": f"{sign}HK${pnl * shares * cost / 100:,.0f}（{sign}{pnl:.1f}%）", "inline": True},
            {"name": "止损价", "value": f"`HK${stop:.2f}`（成本价）", "inline": True},
            {"name": "操作计划", "value": "分批减仓", "inline": False},
        ]
        embeds.append(_build_embed(
            title=f"📌 {name} ({code})",
            color=DISCORD_COLORS["hk_alert"],
            fields=fields,
            footer=_footer("⚠️ 港股遗留仓位独立管理，不补仓不加仓", ts),
        ))

    return embeds


def _build_hk_alert_embed(
    position: dict,
    current_price: float,
    alert_type: str,
    details: str,
    ts: str = "",
) -> list[dict]:
    """港股单条告警卡。"""
    if not ts:
        ts = datetime.now().strftime("%H:%M")
    name   = position.get("name", "")
    code   = position.get("code", "")
    shares = position.get("shares", 0)
    cost   = position.get("avg_cost", 0)
    pnl    = (current_price / cost - 1) * 100 if cost > 0 else 0
    sign   = "+" if pnl >= 0 else ""
    is_stop = "止损" in alert_type

    return [_build_embed(
        title=f"{'🔴' if is_stop else '🟡'} 港股告警 — {name} ({code})",
        description=details,
        color=DISCORD_COLORS["hk_alert"] if is_stop else 0xFB8C00,
        fields=[
            {"name": "类型", "value": alert_type, "inline": True},
            {"name": "持仓", "value": f"`{shares}` 股 @ `HK${cost:.2f}`", "inline": True},
            {"name": "现价", "value": f"`HK${current_price:.2f}`（{sign}{pnl:.1f}%）", "inline": True},
        ],
        footer=_footer("⚠️ 港股遗留仓位独立管理，不补仓不加仓", ts),
        author_name="Hermes Cron · hk_monitor",
    )]


# ---------------------------------------------------------------------------
# 模板函数（兼容旧版纯文本，保留供 hk_monitor 直接调用）
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
        lines.append(f"  🔔 {_signal_emoji_cn(signal)}")

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
        signal_mark = f"🔔 {_signal_cn(signal)}" if signal else ""
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
    盘前摘要（8:25）→ Discord Rich Embed

    data 字段：
      date, weekday, market_signal
      market: {名称: {price, chg_pct, ma20_pct, ma60_pct, ma60_days, signal}}
      positions: [{name, shares, price, currency, note}]
      core_pool: [{name, score, note}]
      weekly_bought, weekly_limit
    """
    embeds = _build_morning_embeds(data)
    return _post_embed_to_discord(embeds)


def send_noon_check(data: dict) -> Tuple[bool, str]:
    """
    午休检查（11:55）→ Discord Rich Embed

    data 字段：
      date, weekday
      market: {名称: {price, chg_pct, high, low}}
      positions: [{name, shares, cost, price, pnl_pct, currency}]
      tips: [str, ...]
    """
    embeds = _build_noon_embeds(data)
    return _post_embed_to_discord(embeds)


def send_evening_report(data: dict) -> Tuple[bool, str]:
    """
    收盘报告（15:35）→ Discord Rich Embed

    data 字段：
      date, weekday, currency
      market: {名称: {price, chg_pct, signal}}
      positions: [{name, shares, value, currency, status}]
      total_value
      alerts: [str, ...]
      core_pool: [{name, score, note}]
      tomorrow_plan: [str, ...]
    """
    embeds = _build_evening_embeds(data)
    return _post_embed_to_discord(embeds)


def send_weekly_report(data: dict) -> Tuple[bool, str]:
    """
    周报（周日20:00）→ Discord Rich Embed

    data 字段：
      year, week, currency
      pnl_pct, pnl_abs
      win_rate, trades, profit_loss_ratio
      position_changes: [{action, name, shares, price, currency}]
      core_pool_changes: [{name, old_score, new_score, reason}]
      next_week_plan: [str, ...]
    """
    embeds = _build_weekly_embeds(data)
    return _post_embed_to_discord(embeds)


def send_sentiment_alert(data: dict) -> Tuple[bool, str]:
    """
    舆情提醒 → Discord Rich Embed

    data 字段：
      matched_keywords: [str, ...]
      source, title, summary, url
      sentiment: positive / negative / neutral
    """
    embeds = _build_sentiment_embeds(data)
    return _post_embed_to_discord(embeds)


def send_condition_order_reminder(pending: list) -> Tuple[bool, str]:
    """
    条件单待确认提醒 → Discord Rich Embed

    pending 元素：
      {name, type, price, currency, status}
    """
    # 保留旧文本格式（条件单提醒暂不升级）
    content = _build_condition_order_reminder(pending)
    return _post_to_discord(content)


def render_condition_order_reminder(pending: list) -> str:
    """Render the condition-order reminder content without sending it."""
    return _build_condition_order_reminder(pending)
