#!/usr/bin/env python3
from __future__ import annotations

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
from typing import Optional, Union, Dict, Tuple

try:
    import yaml
except ImportError:
    yaml = None  # PyYAML optional; notification.yaml features disabled


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------

def _discord_escape(text: str) -> str:
    """
    转义 Discord Markdown 特殊字符，防止 LLM 生成内容破坏卡片格式。

    Discord embeds 中，以下字符在 value/name/description 里会被解析为 markdown：
    *  _italic_  (单星号/下划线)
    *  **bold**
    *  ``` code block ```
    *  `inline code`
    *  > blockquote
    *  # heading
    *  | spoiler |
    *  ~strikethrough~
    """
    if not text:
        return text
    text = text.replace("\\", "\\\\")
    text = text.replace("*", "\\*")
    text = text.replace("_", "\\_")
    text = text.replace("`", "\\`")
    text = text.replace(">", "\\>")
    text = text.replace("#", "\\#")
    text = text.replace("|", "\\|")
    text = text.replace("~", "\\~")
    return text


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


def _load_bot_token() -> str:
    """
    优先读取环境变量 DISCORD_BOT_TOKEN，
    否则从 config/notification.yaml 的 discord.bot_token 读取。
    """
    token = os.environ.get("DISCORD_BOT_TOKEN", "").strip()
    if token:
        return token

    yaml_path = _get_project_root() / "config" / "notification.yaml"
    if yaml_path.exists() and yaml is not None:
        try:
            with open(yaml_path, encoding="utf-8") as f:
                data = yaml.safe_load(f)
            return (data.get("discord", {}) or {}).get("bot_token", "").strip()
        except Exception:
            pass
    return ""


def _load_channel_id() -> str:
    """
    优先读取环境变量 DISCORD_CHANNEL_ID，
    否则从 config/notification.yaml 的 discord.channel_id 读取。
    """
    channel_id = os.environ.get("DISCORD_CHANNEL_ID", "").strip()
    if channel_id:
        return channel_id

    yaml_path = _get_project_root() / "config" / "notification.yaml"
    if yaml_path.exists() and yaml is not None:
        try:
            with open(yaml_path, encoding="utf-8") as f:
                data = yaml.safe_load(f)
            return (data.get("discord", {}) or {}).get("channel_id", "").strip()
        except Exception:
            pass
    return ""


def _load_dm_user_id() -> str:
    """
    读取 DISCORD_DM_USER_ID，优先于 DISCORD_CHANNEL_ID。
    设置此变量后，Bot 会直接 DM 指定用户，而不是发到服务器频道。
    """
    return os.environ.get("DISCORD_DM_USER_ID", "").strip()


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
    "stop_alert": 0xC62828,  # 红色 — 止损告警
    "profit_alert": 0x2E7D32,  # 深绿 — 止盈告警
    "hk_alert":  0x880E4F,  # 深红 — 港股告警
    "hk_summary":0x4A148C,  # 紫黑 — 港股汇总
    "trade_fill": 0x1565C0,  # 蓝色 — 实盘成交确认
    "scoring":    0x00838F,  # 青色 — 核心池评分
    "shadow":     0x4E342E,  # 棕色 — 模拟盘报告
    "info":      0x37474F,  # 灰蓝 — 通用信息
}


def _post_embed_to_discord(
    embeds: list[dict],
    content: str = "",
    username: str = "Hermes 交易系统",
    avatar_url: str = "",
) -> Tuple[bool, str]:
    """
    将 Discord 原生 Rich Embed POST 到 webhook 或 Bot Token API。

    优先级（从高到低）：
      1. DISCORD_DM_USER_ID → Bot Token DM 给用户（最优先）
      2. DISCORD_BOT_TOKEN + DISCORD_CHANNEL_ID → Bot Token 发到频道
      3. DISCORD_WEBHOOK_URL → Webhook 模式（已弃用）
    """
    bot_token = _load_bot_token()
    dm_user_id = _load_dm_user_id()

    # 最高优先：DM 模式
    if bot_token and dm_user_id:
        return _post_embed_via_dm(bot_token, dm_user_id, embeds, content)

    # 其次：频道模式
    channel_id = _load_channel_id()
    if bot_token and channel_id:
        return _post_embed_via_bot(bot_token, channel_id, embeds, content)

    # 回退 webhook 模式
    url = _load_webhook_url()
    if not url:
        return False, "Discord: neither bot_token+channel_id nor webhook_url configured"

    # 截断 content
    if len(content) > 2000:
        content = content[:1997] + "..."

    payload = {
        "content": content,
        "embeds": embeds[:10],
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


def _post_embed_via_bot(
    token: str,
    channel_id: str,
    embeds: list[dict],
    content: str = "",
) -> Tuple[bool, str]:
    """
    通过 Discord Bot Token 将 Rich Embed 发送到指定频道。

    使用 aiohttp（异步，更适合生产环境）。
    """
    try:
        import aiohttp
    except ImportError:
        return False, "aiohttp not installed. Run: pip install aiohttp"

    try:
        url = f"https://discord.com/api/v10/channels/{channel_id}/messages"
        headers = {
            "Authorization": f"Bot {token}",
            "Content-Type": "application/json",
            "User-Agent": "AStockTradingBot/1.0",
        }
        payload = {"content": content}
        if embeds:
            payload["embeds"] = embeds[:10]

        async def _do_post():
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, headers=headers, json=payload) as resp:
                    if resp.status not in (200, 201):
                        body = await resp.text()
                        return False, f"Discord API ({resp.status}): {body[:200]}"
                    return True, ""

        # 同步包装
        import asyncio
        return asyncio.run(_do_post())
    except Exception as e:
        return False, f"Discord Bot send failed: {e}"


async def _get_dm_channel_id(token: str, user_id: str) -> str:
    """创建或获取与指定用户的 DM 频道 ID。"""
    import aiohttp
    async with aiohttp.ClientSession() as session:
        r = await session.post(
            "https://discord.com/api/v10/users/@me/channels",
            headers={
                "Authorization": f"Bot {token}",
                "Content-Type": "application/json",
                "User-Agent": "AStockTradingBot/1.0",
            },
            json={"recipient_id": user_id},
        )
        if r.status not in (200, 201):
            body = await r.text()
            raise RuntimeError(f"创建DM频道失败 ({r.status}): {body[:200]}")
        data = await r.json()
        return data["id"]


def _post_embed_via_dm(
    token: str,
    user_id: str,
    embeds: list[dict],
    content: str = "",
) -> Tuple[bool, str]:
    """
    通过 Discord Bot Token 将 Rich Embed DM 给指定用户。

    先通过 POST /users/@me/channels 创建 DM 频道，
    再向该频道发送消息。
    """
    try:
        import aiohttp
    except ImportError:
        return False, "aiohttp not installed. Run: pip install aiohttp"

    try:
        dm_channel_id = _get_dm_channel_id_sync(token, user_id)
        return _post_embed_via_bot(token, dm_channel_id, embeds, content)
    except Exception as e:
        return False, f"Discord DM send failed: {e}"


def _get_dm_channel_id_sync(token: str, user_id: str) -> str:
    """_get_dm_channel_id 的同步包装。"""
    import asyncio
    return asyncio.run(_get_dm_channel_id(token, user_id))


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


def _render_condition_orders(orders: list[dict], max_items: int = 12) -> str:
    """将条件单建议渲染为多行文本字段。"""
    lines = []
    for idx, item in enumerate(orders):
        if idx >= max_items:
            lines.append(f"… 其余 {len(orders) - max_items} 笔见日志")
            break
        name = str(item.get("name", "")).strip() or "未命名"
        order_type = str(item.get("type", "条件单")).strip() or "条件单"
        currency = str(item.get("currency", "¥")).strip() or "¥"
        quantity = str(item.get("quantity", "")).strip()
        note = str(item.get("note", "")).strip()
        try:
            price_text = f"{currency}{float(item.get('price', 0) or 0):.2f}"
        except (TypeError, ValueError):
            raw_price = str(item.get("price", "")).strip()
            price_text = raw_price or "—"
        line = f"• {name} {order_type} @ `{price_text}`"
        if quantity:
            line += f" · {quantity}"
        if note:
            line += f" · {note}"
        lines.append(line)
    return "\n".join(lines) if lines else "无挂单建议"


def _build_embed(
    title: str = "",
    description: str = "",
    color: int = 0x37474F,
    fields: Optional[list[dict]] = None,
    footer: Optional[dict] = None,
    thumbnail_url: str = "",
    image_url: str = "",
    author_name: str = "",
    author_icon: str = "",
    url: str = "",
    timestamp: str = "",
) -> dict:
    """组装一条 Discord Embed。"""
    embed: dict = {"color": color}
    if title:
        embed["title"] = title[:256]
    if description:
        embed["description"] = description[:4096]
    if url:
        embed["url"] = url
    if fields:
        # 直接使用已构造好的 field dict；仅对缺少 inline 的做兜底
        sanitized = []
        for f in fields:
            sanitized.append({
                "name": str(f.get("name", ""))[:256],
                "value": str(f.get("value", ""))[:1024],
                "inline": f.get("inline", True),
            })
        embed["fields"] = sanitized
    if footer:
        embed["footer"] = footer
    if thumbnail_url:
        embed["thumbnail"] = {"url": thumbnail_url}
    if image_url:
        embed["image"] = {"url": image_url}
    if author_name:
        embed["author"] = {"name": author_name, "icon_url": author_icon} if author_icon else {"name": author_name}
    if timestamp:
        embed["timestamp"] = timestamp
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


def _data_quality_note(stock: dict) -> str:
    """评分数据质量提示。"""
    quality = str(stock.get("data_quality", "ok") or "ok").strip().lower()
    if quality in {"", "ok"}:
        return ""
    missing = stock.get("data_missing_fields") or stock.get("missing_fields") or []
    if isinstance(missing, str):
        missing_text = missing.strip()
    else:
        missing_text = ",".join(str(item).strip() for item in missing if str(item).strip())
    if quality == "degraded":
        note = "⚠️ 数据降级"
    elif quality == "error":
        note = "⚠️ 数据错误"
    else:
        note = f"⚠️ 数据质量:{quality}"
    if missing_text:
        note += f"（缺失:{missing_text}）"
    return note


def _fmt_pct(v: float) -> str:
    """浮点数 → 带符号百分比字符串，如 +0.15%"""
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.2f}%"


def _fmt_price(code: str, price: float, currency: str = "¥") -> str:
    """格式化价格"""
    return f"{currency}{price:.2f}"


def _now_iso() -> str:
    """当前时间 ISO 8601 格式（Discord embed timestamp 用）。"""
    return datetime.now().astimezone().isoformat()


def _pnl_emoji(v: float) -> str:
    """盈亏 → emoji"""
    return "🟢" if v >= 0 else "🔴"


def _pnl_sign(v: float) -> str:
    """盈亏 → 带符号前缀"""
    return "+" if v >= 0 else ""


# ---------------------------------------------------------------------------
# Embed Builder — 盘前摘要
# ---------------------------------------------------------------------------

def _build_morning_embeds(data: dict) -> list[dict]:
    """盘前摘要 → 单张卡片 embed。"""
    date_str = data.get("date", datetime.now().strftime("%Y-%m-%d"))
    weekday = data.get("weekday", "")
    if weekday:
        date_str = f"{date_str}（{weekday}）"
    ts = datetime.now().strftime("%H:%M")
    iso_ts = _now_iso()

    market_signal = data.get("market_signal", "")
    signal_tag = _signal_emoji_cn(market_signal) if market_signal else "—"

    fields: list[dict] = []

    # ── 大盘指数 ──────────────────────────────────────────────────
    for name, info in data.get("market", {}).items():
        price  = info.get("price", 0)
        chg    = info.get("chg_pct", 0)
        ma20   = info.get("ma20_pct", 0)
        ma60   = info.get("ma60_pct", 0)
        sig    = info.get("signal", "")
        sig_tag_item = _signal_emoji_cn(sig) if sig else ""
        field_lines = [
            f"`{price:.2f}` ({_fmt_pct(chg)})",
            f"MA20 {_fmt_pct(ma20)} {'📈' if ma20 >= 0 else '📉'}",
            f"MA60 {_fmt_pct(ma60)} {'📈' if ma60 >= 0 else '📉'}",
        ]
        fields.append({
            "name": f"{sig_tag_item} {name}" if sig_tag_item else name,
            "value": "\n".join(field_lines),
            "inline": True,
        })

    # ── 持仓 ──────────────────────────────────────────────────────
    fields.append({"name": "\u200b", "value": "**💼 持仓**", "inline": False})
    positions = data.get("positions", [])
    if positions:
        for pos in positions:
            name  = pos.get("name", "")
            shares = pos.get("shares", 0)
            price = pos.get("price", 0)
            currency = pos.get("currency", "¥")
            note = pos.get("note", "")
            val = f"{shares} 股 @ `{currency}{price:.2f}`"
            if note:
                val += f"\n_{note}_"
            fields.append({"name": name, "value": val, "inline": True})
    else:
        fields.append({"name": "空仓", "value": "\u200b", "inline": True})

    condition_orders = data.get("condition_orders", [])
    if condition_orders:
        fields.append({
            "name": f"⏰ 条件单预览（{len(condition_orders)} 笔）",
            "value": _render_condition_orders(condition_orders),
            "inline": False,
        })

    # ── 核心池 ────────────────────────────────────────────────────
    core = data.get("core_pool", [])
    if core:
        fields.append({"name": "\u200b", "value": "**🎯 核心池**", "inline": False})
        for stock in core:
            name  = stock.get("name", "")
            score = stock.get("score", 0)
            note  = stock.get("note", "")
            emoji = _score_emoji(score)
            val = f"{emoji} **{score:.1f}**"
            quality_note = _data_quality_note(stock)
            if quality_note:
                val += f"\n{quality_note}"
            if note:
                val += f"\n_{note}_"
            fields.append({"name": name, "value": val, "inline": True})

    # ── 交易计划 ──────────────────────────────────────────────────
    wb  = data.get("weekly_bought", 0)
    wl  = data.get("weekly_limit", 2)
    rem = wl - wb
    status = "✅ 可买入" if rem > 0 else "⚠️ 额度用完"
    fields.append({
        "name": "📋 交易计划",
        "value": f"本周已买 **{wb}/{wl}** · 剩余 **{rem}** 次 · {status}",
        "inline": False,
    })

    return [_build_embed(
        title=f"📊 盘前摘要 — {date_str}",
        description=f"综合信号 **{signal_tag}**",
        color=DISCORD_COLORS["morning"],
        fields=fields,
        footer=_footer("Hermes · morning_brief", ts),
        author_name="Hermes 交易系统",
        timestamp=iso_ts,
    )]


# ---------------------------------------------------------------------------
# Embed Builder — 午休检查
# ---------------------------------------------------------------------------

def _build_noon_embeds(data: dict) -> list[dict]:
    """午休检查 → 单张卡片 embed。"""
    date_str = data.get("date", datetime.now().strftime("%Y-%m-%d"))
    weekday  = data.get("weekday", "")
    if weekday:
        date_str = f"{date_str}（{weekday}）"
    ts = datetime.now().strftime("%H:%M")
    iso_ts = _now_iso()

    fields: list[dict] = []

    # ── 大盘 ──────────────────────────────────────────────────────
    for name, info in data.get("market", {}).items():
        price = info.get("price", 0)
        chg   = info.get("chg_pct", 0)
        high  = info.get("high", 0)
        low   = info.get("low", 0)
        lines = [f"`{price:.2f}` ({_fmt_pct(chg)})"]
        if high and low:
            lines.append(f"振幅 `{low:.2f}` ~ `{high:.2f}`")
        fields.append({"name": name, "value": "\n".join(lines), "inline": True})

    # ── 持仓 ──────────────────────────────────────────────────────
    positions = data.get("positions", [])
    if positions:
        fields.append({"name": "\u200b", "value": "**💼 持仓**", "inline": False})
        for pos in positions:
            name   = pos.get("name", "")
            shares = pos.get("shares", 0)
            cost   = pos.get("cost", 0)
            price  = pos.get("price", 0)
            pnl    = pos.get("pnl_pct", 0)
            cur    = pos.get("currency", "¥")
            emoji  = _pnl_emoji(pnl)
            sign   = _pnl_sign(pnl)
            fields.append({
                "name": f"{emoji} {name}",
                "value": (
                    f"`{shares}` 股 · 成本 `{cur}{cost:.2f}`\n"
                    f"现价 `{cur}{price:.2f}` · 盈亏 **{sign}{pnl:.2f}%**"
                ),
                "inline": True,
            })
    else:
        fields.append({"name": "💼 持仓", "value": "空仓", "inline": False})

    # ── 提示 ──────────────────────────────────────────────────────
    tips = data.get("tips", [])
    tips_value = "\n".join(f"• {t}" for t in tips) if tips else "无特殊提示"
    fields.append({"name": "📋 提示", "value": tips_value, "inline": False})

    return [_build_embed(
        title=f"☀️ 午休检查 — {date_str}",
        color=DISCORD_COLORS["noon"],
        fields=fields,
        footer=_footer("Hermes · noon_check", ts),
        author_name="Hermes 交易系统",
        timestamp=iso_ts,
    )]


# ---------------------------------------------------------------------------
# Embed Builder — 收盘报告
# ---------------------------------------------------------------------------

def _build_evening_embeds(data: dict) -> list[dict]:
    """收盘报告 → 单张卡片 embed。"""
    date_str = data.get("date", datetime.now().strftime("%Y-%m-%d"))
    weekday  = data.get("weekday", "")
    if weekday:
        date_str = f"{date_str}（{weekday}）"
    ts = datetime.now().strftime("%H:%M")
    iso_ts = _now_iso()

    fields: list[dict] = []

    # ── 大盘 ──────────────────────────────────────────────────────
    for name, info in data.get("market", {}).items():
        price = info.get("price", 0)
        chg   = info.get("chg_pct", 0)
        sig   = info.get("signal", "")
        sig_tag = _signal_emoji_cn(sig) if sig else ""
        fields.append({
            "name": name,
            "value": f"`{price:.2f}` ({_fmt_pct(chg)})" + (f"\n{sig_tag}" if sig_tag else ""),
            "inline": True,
        })

    # ── 持仓 ──────────────────────────────────────────────────────
    positions = data.get("positions", [])
    total_value = data.get("total_value", 0)
    cur = data.get("currency", "¥")

    if positions:
        total_line = f"账户总值 **{cur}{total_value:,.0f}**" if total_value else ""
        fields.append({"name": "\u200b", "value": f"**💼 持仓**" + (f" · {total_line}" if total_line else ""), "inline": False})
        for pos in positions:
            pname  = pos.get("name", "")
            shares = pos.get("shares", 0)
            value  = pos.get("value", 0)
            p_cur  = pos.get("currency", "¥")
            status = pos.get("status", "持有中")
            fields.append({
                "name": pname,
                "value": f"`{shares}` 股 · **{p_cur}{value:,.0f}**\n{status}",
                "inline": True,
            })
    elif total_value:
        fields.append({"name": "💼 持仓", "value": f"A股空仓 · 账户总值 **{cur}{total_value:,.0f}**", "inline": False})

    # ── 触发事项 ──────────────────────────────────────────────────
    alerts = data.get("alerts", [])
    if alerts:
        alerts_value = "\n".join(f"• {a}" for a in alerts)
        fields.append({"name": f"⚠️ 触发事项（{len(alerts)} 项）", "value": alerts_value, "inline": False})

    condition_orders = data.get("condition_orders", [])
    if condition_orders:
        fields.append({
            "name": f"⏰ 明日挂单建议（{len(condition_orders)} 笔）",
            "value": _render_condition_orders(condition_orders),
            "inline": False,
        })

    # ── 核心池 ────────────────────────────────────────────────────
    core = data.get("core_pool", [])
    if core:
        fields.append({"name": "\u200b", "value": "**🎯 核心池评分**", "inline": False})
        for stock in core:
            sname = stock.get("name", "")
            score = stock.get("score", 0)
            note  = stock.get("note", "")
            emoji = _score_emoji(score)
            val = f"{emoji} **{score:.1f}**"
            quality_note = _data_quality_note(stock)
            if quality_note:
                val += f"\n{quality_note}"
            if note:
                val += f"\n_{note}_"
            fields.append({"name": sname, "value": val, "inline": True})

    # ── 明日计划 ──────────────────────────────────────────────────
    plan = data.get("tomorrow_plan", [])
    plan_value = "\n".join(f"• {item}" for item in plan) if plan else "暂无计划"
    fields.append({"name": "📋 明日计划", "value": plan_value, "inline": False})

    return [_build_embed(
        title=f"📈 收盘报告 — {date_str}",
        color=DISCORD_COLORS["evening"],
        fields=fields,
        footer=_footer("Hermes · close_review", ts),
        author_name="Hermes 交易系统",
        timestamp=iso_ts,
    )]


# ---------------------------------------------------------------------------
# Embed Builder — 周报
# ---------------------------------------------------------------------------

def _build_weekly_embeds(data: dict) -> list[dict]:
    """周报 → 单张卡片 embed。"""
    week_str = data.get("week", datetime.now().strftime("W%W"))
    year_str = data.get("year", datetime.now().strftime("%Y"))
    ts = datetime.now().strftime("%H:%M")
    iso_ts = _now_iso()

    fields: list[dict] = []

    # ── 收益总览 ──────────────────────────────────────────────────
    pnl_pct = data.get("pnl_pct", 0)
    pnl_abs = data.get("pnl_abs", 0)
    cur     = data.get("currency", "¥")
    emoji   = _pnl_emoji(pnl_pct)
    sign    = _pnl_sign(pnl_pct)

    win_rate = data.get("win_rate", 0)
    if isinstance(win_rate, (int, float)) and win_rate > 1:
        win_rate_str = f"{win_rate:.0f}%"
    else:
        win_rate_str = f"{win_rate:.0%}"

    fields.append({"name": "本周收益", "value": f"{emoji} **{sign}{pnl_pct:.2f}%** ({cur}{pnl_abs:+,.0f})", "inline": False})
    fields.append({"name": "胜率", "value": f"`{win_rate_str}`", "inline": True})
    fields.append({"name": "交易", "value": f"`{data.get('trades', 0)} 笔`", "inline": True})
    fields.append({"name": "盈亏比", "value": f"`{data.get('profit_loss_ratio', 0):.2f}`", "inline": True})

    # ── 持仓变化 ──────────────────────────────────────────────────
    changes = data.get("position_changes", [])
    if changes:
        emap = {"buy": "▲ 买入", "sell": "▼ 卖出", "hold": "— 持有"}
        change_lines = []
        for entry in changes:
            action = entry.get("action", "hold")
            name   = entry.get("name", "")
            shares = entry.get("shares", 0)
            price  = entry.get("price", 0)
            c      = entry.get("currency", "¥")
            tag    = emap.get(action, action)
            change_lines.append(f"{tag} **{name}** · `{shares}` 股 @ `{c}{price:.2f}`")
        fields.append({"name": f"💼 持仓变化（{len(changes)} 笔）", "value": "\n".join(change_lines), "inline": False})

    # ── 核心池异动 ────────────────────────────────────────────────
    core_changes = data.get("core_pool_changes", [])
    if core_changes:
        fields.append({"name": "\u200b", "value": "**🎯 核心池异动**", "inline": False})
        for stock in core_changes:
            name      = stock.get("name", "")
            old_score = stock.get("old_score", 0)
            new_score = stock.get("new_score", 0)
            reason    = stock.get("reason", "")
            chg       = new_score - old_score
            chg_mark  = "▲" if chg > 0 else "▼" if chg < 0 else "—"
            sign_chg  = f"+{chg:.1f}" if chg >= 0 else f"{chg:.1f}"
            val = f"{chg_mark} `{old_score:.1f}` → `{new_score:.1f}` ({sign_chg})"
            if reason:
                val += f"\n_{reason}_"
            fields.append({"name": name, "value": val, "inline": True})

    # ── 下周计划 ──────────────────────────────────────────────────
    next_plan = data.get("next_week_plan", [])
    plan_value = "\n".join(f"• {item}" for item in next_plan) if next_plan else "暂无计划"
    fields.append({"name": "📋 下周计划", "value": plan_value, "inline": False})

    return [_build_embed(
        title=f"📋 周报 — {year_str} {week_str}",
        color=DISCORD_COLORS["weekly"],
        fields=fields,
        footer=_footer("Hermes · weekly_review", ts),
        author_name="Hermes 交易系统",
        timestamp=iso_ts,
    )]


# ---------------------------------------------------------------------------
# Embed Builder — 核心池评分
# ---------------------------------------------------------------------------

def _build_scoring_embeds(scores: list, date_str: str = "") -> list[dict]:
    """
    核心池评分报告 → Discord Embed 卡片。

    scores 元素: {name, code, total_score, technical_score, fundamental_score,
                  flow_score, sentiment_score, data_quality, data_missing_fields,
                  veto_signals, ...}
    """
    if not date_str:
        date_str = datetime.now().strftime("%Y-%m-%d")
    ts = datetime.now().strftime("%H:%M")
    iso_ts = _now_iso()

    weekday_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    weekday = weekday_names[dt.weekday()]

    # 分类：✅可买入 / 🟡观察 / ❌规避
    buy_list, watch_list, avoid_list = [], [], []
    for s in scores:
        from scripts.engine.scorer import get_recommendation
        rec = get_recommendation(s)
        s["_rec"] = rec
        if "✅" in rec:
            buy_list.append(s)
        elif "🟡" in rec or "🟠" in rec:
            watch_list.append(s)
        else:
            avoid_list.append(s)

    fields: list[dict] = []

    # 综合统计
    total = len(scores)
    buy_n = len(buy_list)
    watch_n = len(watch_list)
    avg = sum(float(s.get("total_score", 0) or 0) for s in scores) / total if total else 0

    stats = (
        f"✅ **{buy_n}** 可买入  "
        f"🟡 **{watch_n}** 观察  "
        f"❌ **{total - buy_n - watch_n}** 规避  "
        f"  平均 **{avg:.1f}** 分"
    )
    fields.append({"name": "📊 核心池统计", "value": stats, "inline": False})

    def _score_rows(items: list, emoji: str, label: str) -> list:
        rows = []
        for s in items:
            name = s.get("name", "")
            code = s.get("code", "")
            total_s = s.get("total_score", 0)
            tech = s.get("technical_score", 0)
            fin = s.get("fundamental_score", 0)
            flow = s.get("flow_score", 0)
            sent = s.get("sentiment_score", 0)
            dq = s.get("data_quality", "ok")
            dq_note = " ⚠️数据" if dq != "ok" else ""
            rows.append(
                f"{emoji} **{name}**({code}) "
                f"技术{tech:.1f}/基本面{fin:.1f}/资金{flow:.1f}/舆情{sent:.1f} "
                f"**总分{total_s:.1f}**{dq_note}"
            )
        return rows

    if buy_list:
        fields.append({"name": "✅ 可买入", "value": "\n".join(_score_rows(buy_list, "✅", "可买入")) or "—", "inline": False})
    if watch_list:
        fields.append({"name": "🟡 观察", "value": "\n".join(_score_rows(watch_list, "🟡", "观察")) or "—", "inline": False})
    if avoid_list:
        # 避免过长，只展示前5
        for s in avoid_list[:5]:
            fields.append({
                "name": f"❌ {s.get('name', '')}({s.get('code', '')})",
                "value": f"总分 **{s.get('total_score', 0):.1f}**  {_score_rows([s], '❌', '')[0]}",
                "inline": False,
            })
        if len(avoid_list) > 5:
            fields.append({"name": "…", "value": f"另有 {len(avoid_list) - 5} 只评分 < 5", "inline": False})

    # 颜色：综合评分越高越绿
    score_color = 0x2E7D32 if avg >= 7 else (0xFB8C00 if avg >= 5 else 0xC62828)

    return [_build_embed(
        title=f"🎯 核心池评分 — {date_str}（{weekday}）{ts}",
        description=f"评分 {total} 只 · 平均 **{avg:.1f}** 分",
        color=score_color,
        fields=fields,
        footer=_footer("Hermes · core_pool_scoring", ts),
        author_name="Hermes 交易系统",
        timestamp=iso_ts,
    )]


# ---------------------------------------------------------------------------
# Embed Builder — 舆情提醒
# ---------------------------------------------------------------------------

def _build_sentiment_embeds(data: dict) -> list[dict]:
    """舆情提醒 → 单张卡片 embed。"""
    ts = datetime.now().strftime("%H:%M")
    iso_ts = _now_iso()
    keywords = data.get("matched_keywords", [])
    kw_str    = " / ".join(keywords) if keywords else ""

    sentiment_map = {
        "positive": ("🟢 正面", 0x2E7D32),
        "negative": ("🔴 负面", 0xC62828),
        "neutral":  ("⚪ 中性", 0x37474F),
    }
    sent_tag, sent_color = sentiment_map.get(data.get("sentiment", "neutral"), ("⚪ 中性", 0x37474F))

    title_text = data.get("title", "（无标题）")
    url = data.get("url", "")
    summary = data.get("summary", "")

    desc_lines = [f"**{title_text}**"]
    if url:
        desc_lines.append(f"[查看原文]({url})")
    if summary:
        desc_lines.append(f"\n{summary[:3800]}")

    fields = [
        {"name": "关键词", "value": f"`{kw_str}`" if kw_str else "—", "inline": True},
        {"name": "情绪", "value": sent_tag, "inline": True},
        {"name": "来源", "value": data.get("source", "未知"), "inline": True},
    ]

    return [_build_embed(
        title=f"🔔 舆情提醒 — {kw_str}" if kw_str else "🔔 舆情提醒",
        description="\n".join(desc_lines),
        color=sent_color,
        fields=fields,
        footer=_footer("Hermes · sentiment", ts),
        author_name="Hermes 交易系统",
        timestamp=iso_ts,
    )]


def _build_sentiment_batch_embeds(alerts: list[dict], ts: str = "") -> list[dict]:
    """
    批量舆情提醒 → 单张汇总卡片（多只股票统一推送）。

    alerts 元素：{name, code, level, title, summary, matched_keywords, llm_reason, risk_keywords, url}
    每只股票最多展示 2 条告警，优先使用 LLM 关键词和理由。
    """
    if not ts:
        ts = datetime.now().strftime("%H:%M")
    iso_ts = _now_iso()

    if not alerts:
        return [_build_embed(
            title="🔔 舆情监控 — 无告警",
            description="本次扫描未发现负面舆情",
            color=DISCORD_COLORS["info"],
            footer=_footer("Hermes · sentiment", ts),
            author_name="Hermes 交易系统",
            timestamp=iso_ts,
        )]

    # 按股票聚合，每只最多 2 条
    stock_map: Dict[str, list[dict]] = {}
    for alert in alerts:
        key = alert.get("code", "")
        if key not in stock_map:
            stock_map[key] = []
        if len(stock_map[key]) < 2:
            stock_map[key].append(alert)

    level_colors = {"high": 0xC62828, "warning": 0xFB8C00}
    color = max((level_colors.get(a.get("level", "warning"), 0xFB8C00) for a in alerts), default=0xFB8C00)

    high_count = sum(1 for a in alerts if a.get("level") == "high")
    warn_count = sum(1 for a in alerts if a.get("level") == "warning")
    desc_parts = []
    if high_count:
        desc_parts.append(f"🔴 高危 {high_count} 条")
    if warn_count:
        desc_parts.append(f"🟡 警示 {warn_count} 条")
    desc_parts.append(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    desc_parts.append(f"共 {len(stock_map)} 只股票")

    fields: list[dict] = []
    for code, stock_alerts in stock_map.items():
        for alert in stock_alerts:
            name = alert.get("name", "")
            level = alert.get("level", "warning")
            level_icon = "🔴" if level == "high" else "🟡"
            title_text = _discord_escape(alert.get("title", ""))[:60]
            # 优先用 LLM risk_keywords，其次 keyword 匹配
            keywords = alert.get("risk_keywords") or alert.get("matched_keywords", [])
            kw_str = _discord_escape(" / ".join(keywords[:3])) if keywords else ""
            summary = _discord_escape(alert.get("summary", ""))[:60]
            llm_reason = alert.get("llm_reason", "")
            url = alert.get("url", "")

            # 股票名行为 field name
            stock_field_name = f"{level_icon} {_discord_escape(name)}({code})"
            body_lines = []
            if title_text:
                body_lines.append(f"📰 {title_text}")
            if llm_reason:
                body_lines.append(f"💡 {_discord_escape(llm_reason)}")
            if kw_str:
                body_lines.append(f"`⚠️ {kw_str}`")
            if summary:
                body_lines.append(summary)
            if url:
                body_lines.append(f"[原文]({url})")

            fields.append({
                "name": stock_field_name,
                "value": "\n".join(body_lines)[:1024],
                "inline": False,
            })

    return [_build_embed(
        title=f"🔔 舆情监控 — {high_count}🔴 {warn_count}🟡",
        description=" · ".join(desc_parts),
        color=color,
        fields=fields[:25],
        footer=_footer(f"{len(alerts)} 条告警 · Hermes · sentiment", ts),
        author_name="Hermes 交易系统",
        timestamp=iso_ts,
    )]


# ---------------------------------------------------------------------------
# Embed Builder — 港股遗留仓位
# ---------------------------------------------------------------------------

def _build_hk_embeds(positions: list[dict], ts: str = "") -> list[dict]:
    """港股监控汇总卡（每只股票一张子卡片）。"""
    if not ts:
        ts = datetime.now().strftime("%H:%M")
    iso_ts = _now_iso()
    embeds: list[dict] = []

    embeds.append(_build_embed(
        title=f"🔍 港股遗留仓位监控（{len(positions)} 只）",
        description=f"📅 {datetime.now().strftime('%Y-%m-%d')}",
        color=DISCORD_COLORS["hk_summary"],
        footer=_footer("Hermes · hk_monitor", ts),
        author_name="Hermes 交易系统",
        timestamp=iso_ts,
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
        sign   = _pnl_sign(pnl)
        emoji  = _pnl_emoji(pnl)

        fields = [
            {"name": "持有", "value": f"`{shares}` 股 @ `HK${cost:.2f}`", "inline": True},
            {"name": "现价", "value": f"`HK${cur_price:.2f}`", "inline": True},
            {"name": "市值", "value": f"**HK${cur_value:,.0f}**", "inline": True},
            {"name": f"盈亏 {emoji}", "value": f"{sign}{pnl:.1f}%", "inline": True},
            {"name": "止损价", "value": f"`HK${stop:.2f}`", "inline": True},
            {"name": "计划", "value": "分批减仓", "inline": True},
        ]
        embeds.append(_build_embed(
            title=f"📌 {name} ({code})",
            color=DISCORD_COLORS["hk_alert"],
            fields=fields,
            footer=_footer("⚠️ 独立管理，不补仓不加仓", ts),
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
    sign   = _pnl_sign(pnl)
    is_stop = "止损" in alert_type

    return [_build_embed(
        title=f"{'🔴' if is_stop else '🟡'} 港股告警 — {name} ({code})",
        description=details,
        color=DISCORD_COLORS["hk_alert"] if is_stop else 0xFB8C00,
        fields=[
            {"name": "类型", "value": alert_type, "inline": True},
            {"name": "持仓", "value": f"`{shares}` 股 @ `HK${cost:.2f}`", "inline": True},
            {"name": "现价", "value": f"`HK${current_price:.2f}` ({sign}{pnl:.1f}%)", "inline": True},
        ],
        footer=_footer("⚠️ 独立管理，不补仓不加仓", ts),
        author_name="Hermes 交易系统",
        timestamp=_now_iso(),
    )]


# ---------------------------------------------------------------------------
# Embed Builder — 条件单提醒
# ---------------------------------------------------------------------------

def _build_condition_order_embeds(pending: list) -> list[dict]:
    """条件单待确认提醒 → 单张卡片 embed。"""
    ts = datetime.now().strftime("%H:%M")
    iso_ts = _now_iso()

    if not pending:
        return [_build_embed(
            title="⏰ 条件单提醒",
            description="无待确认条件单",
            color=DISCORD_COLORS["info"],
            footer=_footer("Hermes · condition_order", ts),
            author_name="Hermes 交易系统",
            timestamp=iso_ts,
        )]

    fields: list[dict] = []
    for item in pending:
        name       = item.get("name", "")
        order_type = item.get("type", "条件单")
        price      = item.get("price", 0)
        currency   = item.get("currency", "¥")
        status     = item.get("status", "待确认")
        if "止损" in order_type:
            tag = "▼ 止损"
        elif "止盈" in order_type:
            tag = "▲ 止盈"
        else:
            tag = order_type
        fields.append({
            "name": name,
            "value": f"{tag} @ `{currency}{price:.2f}` · {status}",
            "inline": True,
        })

    guide = (
        '• "止损触发了 {股票名} 成交¥{价格}"\n'
        '• "止盈触发了 {股票名} 成交¥{价格}"\n'
        '• "取消止损 {股票名}" / "取消止盈 {股票名}"'
    )
    fields.append({"name": "📋 确认方式", "value": guide, "inline": False})

    return [_build_embed(
        title=f"⏰ 条件单提醒（{len(pending)} 笔待确认）",
        color=DISCORD_COLORS["info"],
        fields=fields,
        footer=_footer("Hermes · condition_order", ts),
        author_name="Hermes 交易系统",
        timestamp=iso_ts,
    )]


# ---------------------------------------------------------------------------
# Embed Builder — 止损/止盈触发告警
# ---------------------------------------------------------------------------

def _build_stop_alert_embeds(position_changes: list[dict], ts: str = "") -> list[dict]:
    """
    止损/止盈触发告警 → 每只触发股票一张红色/绿色卡片。

    position_changes 元素：
        name, code, new_price, cost_price, shares,
        stop_loss, absolute_stop, t1_price, triggered: [str]
    """
    if not ts:
        ts = datetime.now().strftime("%H:%M")
    iso_ts = _now_iso()

    triggered_changes = [c for c in position_changes if c.get("triggered")]
    if not triggered_changes:
        return []

    embeds: list[dict] = []
    embeds.append(_build_embed(
        title=f"⚠️ 止损/止盈触发告警（{len(triggered_changes)} 只）",
        description=f"📅 {datetime.now().strftime('%Y-%m-%d')} {ts} · 请尽快确认是否成交",
        color=DISCORD_COLORS["stop_alert"],
        footer=_footer("Hermes · stop_alert", ts),
        author_name="Hermes 交易系统",
        timestamp=iso_ts,
    ))

    for change in triggered_changes:
        name = change.get("name", "")
        code = change.get("code", "")
        new_price = change.get("new_price", 0)
        cost = change.get("cost_price", 0)
        shares = change.get("shares", 0)
        stop_loss = change.get("stop_loss", 0)
        absolute_stop = change.get("absolute_stop", 0)
        t1_price = change.get("t1_price", 0)
        triggered_list = change.get("triggered", [])

        pnl_pct = (new_price / cost - 1) * 100 if cost > 0 else 0
        pnl_sign = "+" if pnl_pct >= 0 else ""
        is_loss = pnl_pct < 0

        # 卡片颜色：止损 → 红色，止盈 → 深绿
        card_color = DISCORD_COLORS["stop_alert"] if is_loss else DISCORD_COLORS["profit_alert"]
        emoji = "🔴" if is_loss else "🟢"
        title_type = "止损触发" if is_loss else "止盈触发"

        fields: list[dict] = [
            {
                "name": "现价",
                "value": f"¥{new_price:.2f}（{pnl_sign}{pnl_pct:.2f}%）",
                "inline": True,
            },
            {
                "name": "成本",
                "value": f"¥{cost:.2f}",
                "inline": True,
            },
            {
                "name": "持有",
                "value": f"{shares} 股",
                "inline": True,
            },
        ]

        # 触发类型
        type_labels = []
        for t in triggered_list:
            if "止损" in t:
                type_labels.append("🔴 止损")
            elif "止盈" in t:
                type_labels.append("🟢 止盈")
        fields.append({
            "name": "触发类型",
            "value": " · ".join(type_labels) if type_labels else " · ".join(triggered_list),
            "inline": False,
        })

        # 条件单价格
        order_lines = []
        if stop_loss > 0:
            order_lines.append(f"动态止损：`¥{stop_loss:.2f}`")
        if absolute_stop > 0:
            order_lines.append(f"绝对止损：`¥{absolute_stop:.2f}`")
        if t1_price > 0:
            order_lines.append(f"止盈(T1)：`¥{t1_price:.2f}`")
        fields.append({
            "name": "条件单价格",
            "value": "\n".join(order_lines),
            "inline": False,
        })

        # 确认方式
        fields.append({
            "name": "📋 确认方式",
            "value": (
                '回复："止损触发了 ' + name + ' 成交¥' + f'{new_price:.2f}"\n'
                '或："止盈触发了 ' + name + ' 成交¥' + f'{new_price:.2f}"\n'
                '或："取消止损 ' + name + '" / "取消止盈 ' + name + '"'
            ),
            "inline": False,
        })

        embeds.append(_build_embed(
            title=f"{emoji} {title_type} — {name}（{code}）",
            color=card_color,
            fields=fields,
            footer=_footer("⚠️ 立即确认是否成交", ts),
            author_name="Hermes 交易系统",
            timestamp=iso_ts,
        ))

    return embeds


def send_stop_alert(position_changes: list[dict]) -> Tuple[bool, str]:
    """
    发送止损/止盈触发告警卡片。
    有触发项才发送，平静退出。
    """
    embeds = _build_stop_alert_embeds(position_changes)
    if not embeds:
        return True, "no_triggered"
    return _post_embed_to_discord(embeds)


# ---------------------------------------------------------------------------
# 纯文本渲染（CLI 展示用，非 Discord 推送）
# ---------------------------------------------------------------------------

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
      condition_orders: [{name, type, price, currency, quantity, note}]
      core_pool: [{name, score, note, data_quality, data_missing_fields}]
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
      condition_orders: [{name, type, price, currency, quantity, note}]
      core_pool: [{name, score, note, data_quality, data_missing_fields}]
      tomorrow_plan: [str, ...]
    """
    embeds = _build_evening_embeds(data)
    return _post_embed_to_discord(embeds)


def send_scoring_report(scores: list, date_str: str = "") -> Tuple[bool, str]:
    """
    核心池评分报告 → Discord Rich Embed。

    scores: batch_score() 返回的评分列表
    date_str: 可选，格式 YYYY-MM-DD
    """
    embeds = _build_scoring_embeds(scores, date_str)
    return _post_embed_to_discord(embeds)


def send_shadow_report(
    positions: list,
    balance: dict,
    total_return_pct: float,
    advisory_summary: dict,
    date_str: str = "",
) -> Tuple[bool, str]:
    """
    模拟盘持仓报告 → Discord Rich Embed。

    positions: get_status()["positions"]
    balance: get_status()["balance"]
    total_return_pct: 总体收益率（%）
    advisory_summary: get_status()["advisory_summary"]
    """
    if not date_str:
        date_str = datetime.now().strftime("%Y-%m-%d")
    ts = datetime.now().strftime("%H:%M")
    iso_ts = _now_iso()

    init = balance.get("init_money", 200000)
    total = balance.get("total_assets", 0)
    available = balance.get("available", 0)
    pos_value = balance.get("position_value", 0)
    total_profit = balance.get("total_profit", 0)

    fields: list[dict] = []

    # 账户概览
    fields.append({
        "name": "💰 账户概览",
        "value": (
            f"总资产 **¥{total:,.0f}**  "
            f"可用 **¥{available:,.0f}**\n"
            f"持仓市值 **¥{pos_value:,.0f}**  "
            f"总收益 **¥{total_profit:,.0f}**（{total_return_pct:+.2f}%）"
        ),
        "inline": False,
    })

    # 持仓明细
    if positions:
        pos_lines = []
        for p in positions:
            pnl = p.get("pnl_pct", 0)
            emoji = "🟢" if pnl >= 0 else "🔴"
            pos_lines.append(
                f"{emoji} **{p['name']}**({p['code']}) "
                f"{p['shares']}股 @ ¥{p['cost']:.2f} "
                f"→ ¥{p['price']:.2f} {pnl:+.1f}%"
            )
        fields.append({"name": "📋 持仓明细", "value": "\n".join(pos_lines), "inline": False})
    else:
        fields.append({"name": "📋 持仓明细", "value": "空仓", "inline": False})

    # 风控提示
    adv = advisory_summary.get("positions", [])
    if adv:
        adv_lines = []
        for item in adv:
            drawdown = item.get("drawdown_pct", 0)
            adv_lines.append(
                f"⚠️ **{item['name']}**({item['code']}) "
                f"回撤 **{drawdown*100:.1f}%**  "
                f"{item.get('summary', '')}"
            )
        fields.append({"name": "🛡️ 风控提示", "value": "\n".join(adv_lines), "inline": False})

    color = 0x2E7D32 if total_return_pct >= 0 else 0xC62828

    embeds = [_build_embed(
        title=f"📈 模拟盘报告 — {date_str} {ts}",
        description=f"初始 ¥{init:,.0f} → 当前 ¥{total:,.0f}（{total_return_pct:+.2f}%）",
        color=color,
        fields=fields,
        footer=_footer("Hermes · shadow_trade", ts),
        author_name="Hermes 交易系统",
        timestamp=iso_ts,
    )]
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


def send_sentiment_batch_alert(alerts: list[dict]) -> Tuple[bool, str]:
    """
    批量舆情提醒 → 单张汇总卡片（统一推送，不碎片化）。

    alerts 元素：{name, code, level, title, summary, matched_keywords, url}
    """
    embeds = _build_sentiment_batch_embeds(alerts)
    return _post_embed_to_discord(embeds)


def send_condition_order_reminder(pending: list) -> Tuple[bool, str]:
    """
    条件单待确认提醒 → Discord Rich Embed

    pending 元素：
      {name, type, price, currency, status}
    """
    embeds = _build_condition_order_embeds(pending)
    return _post_embed_to_discord(embeds)


def render_condition_order_reminder(pending: list) -> str:
    """Render the condition-order reminder content without sending it."""
    return _build_condition_order_reminder(pending)
