"""
reporting/market_formatters.py — 市场数据格式化辅助

抽离可复用的热力图展示逻辑，避免 pipeline 之间互相依赖私有 helper。
"""

from __future__ import annotations


def _format_amount_short(amount: float) -> str:
    if amount >= 1e8:
        return f"{amount / 1e8:.1f}亿"
    return f"{amount / 1e4:.0f}万"


def top_sector_movers(sectors: list[dict], limit: int = 5) -> tuple[list[dict], list[dict]]:
    """返回涨幅前 N 和跌幅前 N。

    `sectors` 默认已按涨跌幅降序排列。跌幅榜需要单独按升序取最小值，
    否则会拿到最接近 0 的下跌板块。
    """
    gainers = [s for s in sectors if s.get("change_pct", 0) > 0][:limit]
    losers = sorted(
        (s for s in sectors if s.get("change_pct", 0) < 0),
        key=lambda s: s.get("change_pct", 0),
    )[:limit]
    return gainers, losers


def format_sector_heatmap_markdown(sectors: list[dict], market_stats: dict = None) -> list[str]:
    """把板块数据格式化为 markdown 表格。

    Args:
        sectors: 板块列表
        market_stats: 全市场升降家数 {"up": int, "down": int, "flat": int, "total": int}
    """
    if not sectors:
        return ["数据获取失败"]

    lines = []
    gainers, losers = top_sector_movers(sectors)

    if gainers:
        lines.append("| 板块 | 涨跌幅 | 成交额 |")
        lines.append("|------|--------|--------|")
        for sector in gainers:
            pct = sector.get("change_pct", 0)
            amount = _format_amount_short(sector.get("amount", 0))
            lines.append(f"| 🔺 {sector.get('name', '')} | `{pct:+.2f}%` | {amount} |")

    if losers:
        lines.append("")
        lines.append("| 板块 | 涨跌幅 | 成交额 |")
        lines.append("|------|--------|--------|")
        for sector in losers:
            pct = sector.get("change_pct", 0)
            amount = _format_amount_short(sector.get("amount", 0))
            lines.append(f"| 🔻 {sector.get('name', '')} | `{pct:+.2f}%` | {amount} |")

    if market_stats and market_stats.get("total", 0) > 0:
        lines.append("")
        lines.append(f"*全市场：🔺 **{market_stats['up']}** | 🔻 **{market_stats['down']}** | ⚪ **{market_stats['flat']}** ({market_stats['total']} 只)*")
    else:
        lines.append("")
        lines.append(f"*共 {len(sectors)} 个板块，涨跌数据仅供板块内参考*")
    return lines


def format_market_signals_markdown(
    hot_stocks: list[dict] | None = None,
    northbound: list[dict] | None = None,
    dragon_tiger: dict | None = None,
    lockup: dict | None = None,
) -> list[str]:
    """把事件型市场信号格式化为日志 markdown。"""
    lines = ["### 市场信号"]

    hot_stocks = hot_stocks or []
    if hot_stocks:
        lines.append("")
        lines.append("**热点题材**")
        for item in hot_stocks[:5]:
            name = item.get("name") or item.get("code", "")
            code = item.get("code", "")
            pct = item.get("change_pct", 0) or 0
            reason = item.get("reason", "")
            lines.append(f"- {name}({code}) `{pct:+.2f}%` {reason}")

    northbound = northbound or []
    if northbound:
        last = northbound[-1]
        hgt = last.get("hgt_yi")
        sgt = last.get("sgt_yi")
        lines.append("")
        lines.append(f"**北向资金** {last.get('time', '')}: 沪股通 {hgt}亿 / 深股通 {sgt}亿")

    stocks = (dragon_tiger or {}).get("stocks", [])
    if stocks:
        lines.append("")
        lines.append("**龙虎榜净买入**")
        for item in stocks[:5]:
            name = item.get("name") or item.get("code", "")
            code = item.get("code", "")
            net = item.get("net_buy_wan", 0) or 0
            reason = item.get("reason", "")
            lines.append(f"- {name}({code}) 净买入 {net:,.0f}万 {reason}")

    upcoming = (lockup or {}).get("upcoming", [])
    if upcoming:
        lines.append("")
        lines.append("**解禁预警**")
        for item in upcoming[:5]:
            ratio = item.get("float_ratio", item.get("ratio", 0)) or 0
            lines.append(f"- {item.get('date', '')} {item.get('type', '')} 占流通股 {ratio}%")

    if len(lines) == 1:
        return []
    return lines
