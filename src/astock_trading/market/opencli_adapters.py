"""Adapters backed by the local opencli command."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import shutil
import subprocess
from typing import Optional

import pandas as pd

from astock_trading.market.models import IndexQuote, StockQuote

from .adapter_utils import (
    _normalize_a_stock_code,
    _normalize_opencli_a_stock_symbol,
    _normalize_xueqiu_symbol,
    _parse_heat_value,
    _split_tags,
    _to_float,
    _to_int,
    _xueqiu_symbol,
)

_logger = logging.getLogger(__name__)

_OPENCLI_HOT_SOURCES = ("xueqiu", "eastmoney", "sinafinance", "ths", "tdx")


class OpenCliFinanceAdapter:
    """Use local opencli to fetch optional market-intel signals."""

    def __init__(
        self,
        executable: str | None = None,
        timeout_seconds: int = 45,
        window: str = "background",
        site_session: str = "persistent",
    ):
        self._executable = executable
        self._timeout_seconds = timeout_seconds
        self._window = window
        self._site_session = site_session

    async def get_xueqiu_hot_stocks(self, limit: int = 10, list_type: str = "10") -> list[dict]:
        return await asyncio.to_thread(self._get_xueqiu_hot_stocks_sync, limit, list_type)

    async def get_cross_platform_hot_stocks(self, limit: int = 10) -> list[dict]:
        return await asyncio.to_thread(self._get_cross_platform_hot_stocks_sync, limit)

    async def get_finance_flash(self, limit: int = 20) -> list[dict]:
        return await asyncio.to_thread(self._get_finance_flash_sync, limit)

    async def get_global_risk_news(self, limit: int = 12) -> list[dict]:
        return await asyncio.to_thread(self._get_global_risk_news_sync, limit)

    async def get_market_announcements(self, limit: int = 20) -> list[dict]:
        return await asyncio.to_thread(self._get_market_announcements_sync, limit)

    async def get_xueqiu_comments(self, symbol: str, limit: int = 10) -> list[dict]:
        return await asyncio.to_thread(self._get_xueqiu_comments_sync, symbol, limit)

    async def get_daily_dragon_tiger(self, trade_date: str | None = None, min_net_buy: float | None = None) -> dict:
        return await asyncio.to_thread(self._get_daily_dragon_tiger_sync, trade_date, min_net_buy)

    async def get_northbound_realtime(self) -> list[dict]:
        return await asyncio.to_thread(self._get_northbound_realtime_sync)

    async def get_hot_sectors(
        self,
        limit: int = 10,
        sector_type: str = "industry",
        sort: str = "change",
    ) -> list[dict]:
        return await asyncio.to_thread(self._get_hot_sectors_sync, limit, sector_type, sort)

    async def search_market_news(self, query: str, limit: int = 10) -> list[dict]:
        return await asyncio.to_thread(self._search_market_news_sync, query, limit)

    async def get_realtime(self, codes: list[str]) -> dict[str, StockQuote]:
        return {}

    async def get_kline(self, code: str, period: str = "daily", count: int = 120) -> Optional[pd.DataFrame]:
        return None

    async def get_index(self, symbols: list[str]) -> dict[str, IndexQuote]:
        return {}

    def _run_opencli(
        self,
        site: str,
        command: str,
        *positionals: str,
        options: dict[str, object] | None = None,
        browser: bool = False,
        timeout_seconds: int | None = None,
    ) -> list[dict]:
        executable = self._executable or shutil.which("opencli")
        if not executable:
            _logger.info("[OpenCliFinance] opencli 未安装，跳过 %s %s", site, command)
            return []

        cmd = [executable, site, command]
        cmd.extend(str(p) for p in positionals if str(p or ""))
        for flag, value in (options or {}).items():
            if value is None or value == "":
                continue
            cmd.extend([flag, str(value)])
        cmd.extend(["-f", "json"])
        if browser:
            cmd.extend(["--window", self._window, "--site-session", self._site_session])

        try:
            completed = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout_seconds or self._timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired:
            _logger.warning("[OpenCliFinance] %s %s 获取超时", site, command)
            return []
        except OSError as e:
            _logger.warning(f"[OpenCliFinance] opencli 执行失败: {e}")
            return []

        if completed.returncode != 0:
            detail = (completed.stderr or completed.stdout or "").strip()
            _logger.warning("[OpenCliFinance] %s %s 返回失败: %s", site, command, detail[:240])
            return []

        try:
            payload = json.loads(completed.stdout or "[]")
        except json.JSONDecodeError as e:
            _logger.warning("[OpenCliFinance] %s %s JSON 解析失败: %s", site, command, e)
            return []

        if isinstance(payload, dict) and payload.get("ok") is False:
            _logger.warning("[OpenCliFinance] %s %s 返回错误: %s", site, command, payload.get("error", "unknown"))
            return []
        rows = payload
        if isinstance(payload, dict):
            rows = payload.get("items") or payload.get("data") or payload.get("rows") or []
        if not isinstance(rows, list):
            return []
        return [item for item in rows if isinstance(item, dict)]

    def _get_xueqiu_hot_stocks_sync(self, limit: int = 10, list_type: str = "10") -> list[dict]:
        safe_limit = max(1, min(int(limit or 10), 50))
        rows = self._run_opencli(
            "xueqiu",
            "hot-stock",
            options={"--limit": safe_limit, "--type": str(list_type or "10")},
            browser=True,
        )
        results = []
        for item in rows:
            results.append(self._normalize_hot_stock_item("xueqiu", item))
        return results

    def _get_cross_platform_hot_stocks_sync(self, limit: int = 10) -> list[dict]:
        safe_limit = max(1, min(int(limit or 10), 50))
        all_items: list[dict] = []
        source_specs = (
            ("xueqiu", "hot-stock", {"--limit": safe_limit, "--type": "10"}),
            ("eastmoney", "hot-rank", {"--limit": safe_limit}),
            ("sinafinance", "stock-rank", {"--market": "cn"}),
            ("ths", "hot-rank", {"--limit": safe_limit}),
            ("tdx", "hot-rank", {"--limit": safe_limit}),
        )
        for source, command, options in source_specs:
            rows = self._run_opencli(source, command, options=options, browser=True)
            for item in rows:
                normalized = self._normalize_hot_stock_item(source, item)
                if normalized.get("code") or normalized.get("name"):
                    all_items.append(normalized)
        return self._aggregate_hot_stocks(all_items, safe_limit)

    def _normalize_hot_stock_item(self, source: str, item: dict) -> dict:
        raw_symbol = str(item.get("symbol") or "").strip()
        tags_raw = item.get("tags", "")
        code = _normalize_opencli_a_stock_symbol(raw_symbol, tags_raw)
        if source == "xueqiu" and not code:
            code = _normalize_xueqiu_symbol(raw_symbol)
        name = str(item.get("name") or code or raw_symbol).strip()
        heat_raw = item.get("heat", "")
        return {
            "rank": _to_int(item.get("rank")),
            "symbol": raw_symbol,
            "code": code,
            "name": name,
            "price": _to_float(item.get("price")),
            "change_pct": _to_float(item.get("changePercent", item.get("change_pct", item.get("change")))),
            "heat": _parse_heat_value(heat_raw),
            "heat_text": str(heat_raw or ""),
            "tags": _split_tags(tags_raw),
            "url": item.get("url", ""),
            "source": source,
        }

    def _aggregate_hot_stocks(self, items: list[dict], limit: int) -> list[dict]:
        groups: dict[str, dict] = {}
        source_order = {name: i for i, name in enumerate(_OPENCLI_HOT_SOURCES)}
        for item in items:
            key = item.get("code") or item.get("name")
            if not key:
                continue
            source = item.get("source", "")
            rank = _to_int(item.get("rank"), 999)
            entry = groups.setdefault(
                key,
                {
                    "code": item.get("code", ""),
                    "name": item.get("name") or key,
                    "price": item.get("price", 0),
                    "change_pct": item.get("change_pct", 0),
                    "best_rank": rank,
                    "score": 0,
                    "sources": [],
                    "source_ranks": {},
                    "tags": [],
                    "items": [],
                },
            )
            if source and source not in entry["sources"]:
                entry["sources"].append(source)
            if source:
                prev_rank = entry["source_ranks"].get(source, 999)
                entry["source_ranks"][source] = min(prev_rank, rank)
            entry["best_rank"] = min(entry["best_rank"], rank)
            entry["score"] += max(1, 60 - rank)
            if not entry.get("price") and item.get("price"):
                entry["price"] = item["price"]
            if item.get("change_pct"):
                entry["change_pct"] = item["change_pct"]
            for tag in item.get("tags", []):
                if tag not in entry["tags"]:
                    entry["tags"].append(tag)
            entry["items"].append(item)

        rows = []
        for entry in groups.values():
            entry["sources"].sort(key=lambda s: source_order.get(s, 99))
            entry["source_count"] = len(entry["sources"])
            rows.append(entry)
        rows.sort(key=lambda x: (-x["source_count"], -x["score"], x["best_rank"], x["name"]))
        for idx, row in enumerate(rows[:limit], start=1):
            row["rank"] = idx
        return rows[:limit]

    def _get_finance_flash_sync(self, limit: int = 20) -> list[dict]:
        safe_limit = max(1, min(int(limit or 20), 50))
        items: list[dict] = []
        for row in self._run_opencli("eastmoney", "kuaixun", options={"--limit": safe_limit}, browser=False):
            title = str(row.get("title") or "").strip()
            summary = str(row.get("summary") or "").strip()
            items.append({
                "time": row.get("time", ""),
                "title": title or summary[:80],
                "summary": summary,
                "content": summary,
                "stocks": row.get("stocks", ""),
                "source": "eastmoney",
            })
        for row in self._run_opencli("sinafinance", "news", options={"--limit": safe_limit}, browser=False):
            content = str(row.get("content") or "").strip()
            title = self._flash_title(content)
            items.append({
                "id": row.get("id"),
                "time": row.get("time", ""),
                "title": title,
                "summary": content,
                "content": content,
                "views": row.get("views", ""),
                "source": "sinafinance",
            })
        return self._dedupe_news_items(items, safe_limit)

    def _flash_title(self, content: str) -> str:
        text = content.strip()
        if text.startswith("【") and "】" in text:
            return text[1:text.index("】")].strip()
        return text[:60]

    def _get_global_risk_news_sync(self, limit: int = 12) -> list[dict]:
        safe_limit = max(1, min(int(limit or 12), 30))
        items: list[dict] = []
        bloomberg_limit = min(5, safe_limit)
        for feed in ("markets", "economics"):
            for row in self._run_opencli("bloomberg", feed, options={"--limit": bloomberg_limit}, browser=False):
                items.append({
                    "title": row.get("title", ""),
                    "summary": row.get("summary", ""),
                    "url": row.get("link", ""),
                    "source": "bloomberg",
                    "channel": feed,
                })
        reuters_limit = min(5, safe_limit)
        for row in self._run_opencli(
            "reuters",
            "search",
            "China markets",
            options={"--limit": reuters_limit},
            browser=True,
            timeout_seconds=min(self._timeout_seconds, 20),
        ):
            items.append({
                "title": row.get("title", ""),
                "summary": "",
                "time": row.get("date", ""),
                "url": row.get("url", ""),
                "source": "reuters",
                "channel": row.get("section", "") or row.get("section_path", ""),
            })
        return self._dedupe_news_items(items, safe_limit)

    def _get_market_announcements_sync(self, limit: int = 20) -> list[dict]:
        safe_limit = max(1, min(int(limit or 20), 100))
        items = []
        for row in self._run_opencli("eastmoney", "announcement", options={"--limit": safe_limit}, browser=False):
            items.append({
                "time": row.get("time", ""),
                "code": _normalize_a_stock_code(str(row.get("code", ""))),
                "name": row.get("name", ""),
                "title": row.get("title", ""),
                "category": row.get("category", ""),
                "url": row.get("url", ""),
                "source": "eastmoney",
            })
        return items[:safe_limit]

    def _get_xueqiu_comments_sync(self, symbol: str, limit: int = 10) -> list[dict]:
        safe_limit = max(1, min(int(limit or 10), 50))
        xq_symbol = _xueqiu_symbol(symbol)
        items = []
        for row in self._run_opencli(
            "xueqiu",
            "comments",
            xq_symbol,
            options={"--limit": safe_limit},
            browser=True,
            timeout_seconds=min(self._timeout_seconds, 30),
        ):
            items.append({
                "author": row.get("author", ""),
                "text": row.get("text", ""),
                "likes": _to_int(row.get("likes")),
                "replies": _to_int(row.get("replies")),
                "retweets": _to_int(row.get("retweets")),
                "created_at": row.get("created_at", ""),
                "url": row.get("url", ""),
                "source": "xueqiu",
            })
        return items

    def _get_daily_dragon_tiger_sync(self, trade_date: str | None = None, min_net_buy: float | None = None) -> dict:
        options: dict[str, object] = {"--limit": 100}
        if trade_date:
            options["--date"] = trade_date
        stocks = []
        for row in self._run_opencli("eastmoney", "longhu", options=options, browser=False):
            net_buy_wan = _to_float(row.get("netAmt")) / 10000
            if min_net_buy is not None and net_buy_wan < min_net_buy:
                continue
            stocks.append({
                "code": _normalize_a_stock_code(str(row.get("code", ""))),
                "name": row.get("name", ""),
                "reason": row.get("reason", ""),
                "close": _to_float(row.get("closePrice")),
                "change_pct": round(_to_float(row.get("changeRate")), 2),
                "net_buy_wan": round(net_buy_wan, 1),
                "buy_wan": round(_to_float(row.get("buyAmt")) / 10000, 1),
                "sell_wan": round(_to_float(row.get("sellAmt")) / 10000, 1),
                "turnover": _to_float(row.get("turnover")),
                "market": row.get("market", ""),
                "trade_date": row.get("tradeDate", ""),
            })
        actual_date = stocks[0].get("trade_date", "") if stocks else trade_date
        return {"date": actual_date, "total_records": len(stocks), "stocks": stocks}

    def _get_northbound_realtime_sync(self) -> list[dict]:
        rows = []
        for row in self._run_opencli("eastmoney", "northbound", options={"--limit": 10}, browser=False):
            rows.append({
                "time": row.get("time", ""),
                "cumulative_net_yi": _to_float(row.get("cumulativeNetYi")),
                "minute_net_yi": _to_float(row.get("minuteNetYi")),
                "total_net_yi": _to_float(row.get("totalNetYi")),
                "source": "eastmoney",
            })
        return rows

    def _get_hot_sectors_sync(
        self,
        limit: int = 10,
        sector_type: str = "industry",
        sort: str = "change",
    ) -> list[dict]:
        safe_limit = max(1, min(int(limit or 10), 100))
        normalized_type = sector_type if sector_type in {"industry", "concept", "region"} else "industry"
        normalized_sort = sort if sort in {"change", "drop", "money-flow", "out-flow", "turnover"} else "change"
        rows = []
        for row in self._run_opencli(
            "eastmoney",
            "sectors",
            options={"--type": normalized_type, "--sort": normalized_sort, "--limit": safe_limit},
            browser=False,
        ):
            rows.append({
                "rank": _to_int(row.get("rank")),
                "code": str(row.get("code") or "").strip(),
                "name": str(row.get("name") or "").strip(),
                "price": _to_float(row.get("price")),
                "change_pct": _to_float(row.get("changePercent", row.get("change_pct"))),
                "main_net": _to_float(row.get("mainNet", row.get("main_net"))),
                "lead_stock": str(row.get("leadStock") or row.get("lead_stock") or "").strip(),
                "lead_change_pct": _to_float(row.get("leadChangePercent", row.get("lead_change_pct"))),
                "up_count": _to_int(row.get("upCount", row.get("up_count"))),
                "down_count": _to_int(row.get("downCount", row.get("down_count"))),
                "type": normalized_type,
                "sort": normalized_sort,
                "source": "eastmoney",
            })
        return rows[:safe_limit]

    def _search_market_news_sync(self, query: str, limit: int = 10) -> list[dict]:
        safe_limit = max(1, min(int(limit or 10), 40))
        query_text = str(query or "").strip()
        tokens = self._market_news_tokens(query_text)
        flash = self._get_finance_flash_sync(limit=max(safe_limit * 3, 20))
        if tokens:
            flash = [item for item in flash if self._news_matches_tokens(item, tokens)]

        items = flash[:safe_limit]
        if self._query_wants_global(query_text):
            reuters_query = query_text if re.search(r"[A-Za-z]", query_text) else "China markets"
            for row in self._run_opencli(
                "reuters",
                "search",
                reuters_query,
                options={"--limit": min(10, safe_limit)},
                browser=True,
                timeout_seconds=min(self._timeout_seconds, 20),
            ):
                items.append({
                    "rank": _to_int(row.get("rank")),
                    "title": row.get("title", ""),
                    "summary": "",
                    "time": row.get("date", ""),
                    "url": row.get("url", ""),
                    "source": "reuters",
                    "channel": row.get("section", "") or row.get("section_path", ""),
                })
        return self._dedupe_news_items(items, safe_limit)

    def _market_news_tokens(self, query: str) -> list[str]:
        text = str(query or "").lower()
        generic_terms = (
            "今天", "今日", "有没有", "什么", "哪些", "那个", "几个", "热点", "新闻", "快讯",
            "板块", "最强", "强势", "财经", "市场", "一下", "看看", "了吗", "是否", "和",
        )
        for term in generic_terms:
            text = text.replace(term, " ")
        tokens = re.findall(r"[a-z0-9][a-z0-9._-]+|[\u4e00-\u9fff]{2,}", text)
        seen = set()
        result = []
        for token in tokens:
            if token not in seen:
                seen.add(token)
                result.append(token)
        return result

    def _query_wants_global(self, query: str) -> bool:
        lowered = str(query or "").lower()
        global_terms = (
            "海外", "全球", "国际", "美国", "美联储", "fed", "rate", "inflation", "china",
            "reuters", "bloomberg", "tariff", "oil", "yield", "macro",
        )
        return bool(re.search(r"[A-Za-z]", lowered)) or any(term in lowered for term in global_terms)

    def _news_matches_tokens(self, item: dict, tokens: list[str]) -> bool:
        haystack = " ".join(
            str(item.get(key) or "").lower()
            for key in ("title", "summary", "content", "stocks", "channel")
        )
        return any(token in haystack for token in tokens)

    def _dedupe_news_items(self, items: list[dict], limit: int) -> list[dict]:
        seen = set()
        deduped = []
        for item in items:
            title = str(item.get("title") or item.get("summary") or "").strip()
            if not title:
                continue
            key = re.sub(r"\s+", "", title)[:80]
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        deduped.sort(key=lambda x: str(x.get("time", "")), reverse=True)
        return deduped[:limit]


class OpenCliXueqiuAdapter(OpenCliFinanceAdapter):
    """Backward-compatible name for the opencli Xueqiu/finance adapter."""
