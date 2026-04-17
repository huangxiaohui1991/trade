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


def format_sector_heatmap_markdown(sectors: list[dict]) -> list[str]:
    """把板块数据格式化为 markdown 表格。"""
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

    total_up = sum(s.get("up_count", 0) for s in sectors)
    total_down = sum(s.get("down_count", 0) for s in sectors)
    lines.append("")
    lines.append(f"*全市场 {len(sectors)} 个板块：上涨 **{total_up}** 个 / 下跌 **{total_down}** 个*")
    return lines
