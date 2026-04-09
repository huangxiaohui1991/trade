#!/usr/bin/env python3
"""
Shared standard reason-code registry and status summary helpers.

The goal is to provide a single compact contract that can be reused by
market, pool, and trade-facing summaries without rewriting each pipeline
output shape.
"""

from __future__ import annotations

import re
from typing import Any

from scripts.engine.scorer import split_veto_signals


REASON_CODE_REGISTRY: dict[str, dict[str, str]] = {
    "MARKET_UNKNOWN": {"category": "market", "label": "大盘状态未知", "state": "unknown"},
    "MARKET_GREEN": {"category": "market", "label": "大盘偏强", "state": "GREEN"},
    "MARKET_YELLOW": {"category": "market", "label": "大盘震荡", "state": "YELLOW"},
    "MARKET_RED": {"category": "market", "label": "大盘转弱", "state": "RED"},
    "MARKET_CLEAR": {"category": "market", "label": "清仓/不抄底", "state": "CLEAR"},
    "POOL_OK": {"category": "pool", "label": "池子正常", "state": "ok"},
    "POOL_EMPTY": {"category": "pool", "label": "池子为空", "state": "ok"},
    "POOL_WARNING": {"category": "pool", "label": "池子预警", "state": "warning"},
    "POOL_WARNING_CONSECUTIVE_OUTFLOW": {
        "category": "pool",
        "label": "连续流出预警",
        "state": "warning",
    },
    "POOL_HARD_VETO": {"category": "pool", "label": "池子硬性禁入", "state": "block"},
    "POOL_SYNC_DRIFT": {"category": "pool", "label": "池子投影漂移", "state": "drift"},
    "TRADE_OK": {"category": "trade", "label": "交易链路正常", "state": "ok"},
    "TRADE_ADVISORY": {"category": "trade", "label": "交易风控提示", "state": "warning"},
    "TRADE_WEEKLY_BUY_LIMIT": {"category": "trade", "label": "周买入上限", "state": "block"},
    "TRADE_EXPOSURE_LIMIT": {"category": "trade", "label": "总仓位限制", "state": "block"},
    "TRADE_HOLDING_LIMIT": {"category": "trade", "label": "持仓数限制", "state": "block"},
    "TRADE_PORTFOLIO_DAILY_LOSS_LIMIT": {"category": "trade", "label": "单日亏损上限", "state": "block"},
    "TRADE_CONSECUTIVE_LOSS_COOLDOWN": {"category": "trade", "label": "连续亏损冷却", "state": "block"},
    "TRADE_POSITION_CONCENTRATION_WARNING": {"category": "trade", "label": "持仓集中度预警", "state": "warning"},
    "TRADE_PAPER_RECONCILE_DRIFT": {"category": "trade", "label": "模拟盘对账漂移", "state": "drift"},
    "TRADE_PAPER_RECONCILE_OPEN": {"category": "trade", "label": "补录缺失开仓", "state": "repair"},
    "TRADE_PAPER_RECONCILE_FLATTEN": {"category": "trade", "label": "补录缺失平仓", "state": "repair"},
    "TRADE_PAPER_RECONCILE_ADD": {"category": "trade", "label": "补录缺失买入", "state": "repair"},
    "TRADE_PAPER_RECONCILE_REDUCE": {"category": "trade", "label": "补录缺失卖出", "state": "repair"},
    "RISK_TIME_STOP": {"category": "trade", "label": "时间止损提示", "state": "warning"},
    "RISK_DRAWDOWN_TAKE_PROFIT": {"category": "trade", "label": "回撤止盈提示", "state": "warning"},
}

REASON_CODE_ALIASES = {
    "consecutive_outflow_warn": "POOL_WARNING_CONSECUTIVE_OUTFLOW",
    "pool_sync_drift": "POOL_SYNC_DRIFT",
    "paper_trade_consistency_drift": "TRADE_PAPER_RECONCILE_DRIFT",
    "paper_trade_drift": "TRADE_PAPER_RECONCILE_DRIFT",
    "risk_time_stop": "RISK_TIME_STOP",
    "risk_drawdown_take_profit": "RISK_DRAWDOWN_TAKE_PROFIT",
    "trade_portfolio_daily_loss_limit": "TRADE_PORTFOLIO_DAILY_LOSS_LIMIT",
    "trade_consecutive_loss_cooldown": "TRADE_CONSECUTIVE_LOSS_COOLDOWN",
    "trade_position_concentration_warning": "TRADE_POSITION_CONCENTRATION_WARNING",
}

TRADE_REASON_PATTERNS = (
    (re.compile(r"^market_signal=(GREEN|YELLOW|RED|CLEAR)$", re.IGNORECASE), "MARKET_{0}"),
    (re.compile(r"本周买入次数已满"), "TRADE_WEEKLY_BUY_LIMIT"),
    (re.compile(r"总仓位已达上限"), "TRADE_EXPOSURE_LIMIT"),
    (re.compile(r"总仓位将超限"), "TRADE_EXPOSURE_LIMIT"),
    (re.compile(r"单只仓位超限"), "TRADE_EXPOSURE_LIMIT"),
    (re.compile(r"持仓只数已达上限"), "TRADE_HOLDING_LIMIT"),
    (re.compile(r"单日已实现亏损达上限"), "TRADE_PORTFOLIO_DAILY_LOSS_LIMIT"),
    (re.compile(r"连续亏损冷却中"), "TRADE_CONSECUTIVE_LOSS_COOLDOWN"),
    (re.compile(r"持仓集中度预警"), "TRADE_POSITION_CONCENTRATION_WARNING"),
    (re.compile(r"^\[?RISK_TIME_STOP\]?$", re.IGNORECASE), "RISK_TIME_STOP"),
    (re.compile(r"^\[?RISK_DRAWDOWN_TAKE_PROFIT\]?$", re.IGNORECASE), "RISK_DRAWDOWN_TAKE_PROFIT"),
)


def _dedupe(items: list[str]) -> list[str]:
    seen = set()
    result = []
    for item in items:
        if not item or item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _prioritize_codes(codes: list[str], priority: list[str]) -> list[str]:
    ordered = []
    remaining = list(codes)
    for item in priority:
        if item in remaining:
            ordered.append(item)
            remaining = [code for code in remaining if code != item]
    ordered.extend(remaining)
    return _dedupe(ordered)


def normalize_reason_code(code: Any, category: str | None = None) -> str:
    text = str(code or "").strip()
    if not text:
        return ""

    alias = REASON_CODE_ALIASES.get(text.lower())
    if alias:
        return alias

    if text.startswith("market_signal="):
        value = text.split("=", 1)[1].strip().upper()
        if value:
            return f"MARKET_{value}"

    cleaned = re.sub(r"[^A-Za-z0-9]+", "_", text).strip("_").upper()
    if not cleaned:
        return ""

    if category:
        prefix = category.strip().upper()
        if cleaned.startswith(prefix):
            return cleaned
    return cleaned


def reason_meta(code: str) -> dict[str, str]:
    normalized = normalize_reason_code(code)
    meta = dict(REASON_CODE_REGISTRY.get(normalized, {}))
    meta.setdefault("code", normalized)
    meta.setdefault("label", normalized)
    meta.setdefault("category", "")
    meta.setdefault("state", "unknown")
    return meta


def summarize_reason_codes(category: str, reason_codes: list[str] | None = None,
                           source_codes: list[str] | None = None,
                           state: str = "",
                           details: dict[str, Any] | None = None) -> dict:
    category = str(category or "").strip().lower()
    codes = _dedupe([normalize_reason_code(code, category=category) for code in (reason_codes or []) if code])
    if not codes:
        default_code = {
            "market": "MARKET_UNKNOWN",
            "pool": "POOL_OK",
            "trade": "TRADE_OK",
        }.get(category, "UNKNOWN")
        codes = [default_code]

    meta = [reason_meta(code) for code in codes]
    primary = codes[0]
    state = state or reason_meta(primary).get("state", "unknown")
    source_codes = _dedupe([normalize_reason_code(code, category=category) for code in (source_codes or []) if code])
    if not source_codes:
        source_codes = []
    summary = "；".join(item["label"] for item in meta if item.get("label")) or primary
    return {
        "category": category,
        "state": state,
        "primary_code": primary,
        "reason_codes": codes,
        "source_codes": source_codes,
        "reason_count": len(codes),
        "labels": [item["label"] for item in meta if item.get("label")],
        "summary": summary,
        "details": details or {},
    }


def _market_summary(market_snapshot: dict | None) -> dict:
    market_snapshot = market_snapshot or {}
    market_signal = str(
        market_snapshot.get("signal", market_snapshot.get("market_signal", "CLEAR"))
    ).strip().upper() or "CLEAR"
    code = normalize_reason_code(f"MARKET_{market_signal}") or "MARKET_UNKNOWN"
    return summarize_reason_codes(
        "market",
        [code],
        source_codes=[market_signal],
        state=market_signal,
        details={
            "source": market_snapshot.get("source", ""),
            "source_chain": list(market_snapshot.get("source_chain", [])),
            "as_of_date": market_snapshot.get("as_of_date", ""),
        },
    )


def _pool_summary(pool_snapshot: dict | None, pool_audit: dict | None = None) -> dict:
    pool_snapshot = pool_snapshot or {}
    pool_audit = pool_audit or {}
    entries = list(pool_snapshot.get("entries", []))
    source_codes: list[str] = []
    reason_codes: list[str] = []
    has_warning = False
    has_hard_veto = False

    for entry in entries:
        hard_veto, warning = split_veto_signals(entry.get("veto_signals", []))
        source_codes.extend(hard_veto)
        source_codes.extend(warning)
        if hard_veto:
            has_hard_veto = True
        if warning:
            has_warning = True

    if any(code == "consecutive_outflow_warn" for code in source_codes):
        reason_codes.append("POOL_WARNING_CONSECUTIVE_OUTFLOW")
    if has_hard_veto:
        reason_codes.append("POOL_HARD_VETO")
    if has_warning:
        reason_codes.append("POOL_WARNING")

    if not entries:
        reason_codes = ["POOL_EMPTY"]

    if pool_audit.get("status") not in {"", "ok"} or not pool_audit.get("ok", True):
        reason_codes.append("POOL_SYNC_DRIFT")

    state = "ok"
    if "POOL_SYNC_DRIFT" in reason_codes:
        state = "drift"
    elif "POOL_HARD_VETO" in reason_codes:
        state = "block"
    elif "POOL_WARNING" in reason_codes or "POOL_WARNING_CONSECUTIVE_OUTFLOW" in reason_codes:
        state = "warning"

    reason_codes = _prioritize_codes(
        reason_codes,
        ["POOL_SYNC_DRIFT", "POOL_HARD_VETO", "POOL_WARNING_CONSECUTIVE_OUTFLOW", "POOL_WARNING", "POOL_EMPTY", "POOL_OK"],
    )
    summary = summarize_reason_codes(
        "pool",
        reason_codes,
        source_codes=source_codes,
        state=state,
        details={
            "snapshot_date": pool_snapshot.get("snapshot_date", ""),
            "updated_at": pool_snapshot.get("updated_at", ""),
            "summary": pool_snapshot.get("summary", {}),
            "audit_status": pool_audit.get("status", ""),
        },
    )
    summary["source_codes"] = _dedupe(summary["source_codes"])
    return summary


def _trade_reason_codes_from_texts(reasons: list[str] | None) -> tuple[list[str], list[str]]:
    source_codes: list[str] = []
    reason_codes: list[str] = []
    for reason in reasons or []:
        text = str(reason or "").strip()
        if not text:
            continue
        matched = None
        for pattern, replacement in TRADE_REASON_PATTERNS:
            match = pattern.search(text)
            if not match:
                continue
            matched = replacement.format(*match.groups())
            break
        if matched:
            if matched.startswith("MARKET_"):
                source_codes.append(matched)
                continue
            reason_codes.append(matched)
        else:
            source_codes.append(text)
    return _dedupe(reason_codes), _dedupe(source_codes)


def _trade_summary(today_decision: dict | None, shadow_snapshot: dict | None) -> dict:
    today_decision = today_decision or {}
    shadow_snapshot = shadow_snapshot or {}

    explicit_reason_codes = _dedupe(
        [
            normalize_reason_code(code, category="trade")
            for code in (today_decision.get("reason_codes", []) or [])
            if code
        ]
    )
    text_reason_codes, source_codes = _trade_reason_codes_from_texts(today_decision.get("reasons", []))
    reason_codes = _dedupe(explicit_reason_codes + text_reason_codes)

    consistency = shadow_snapshot.get("consistency", {}) or {}
    advisory_summary = shadow_snapshot.get("advisory_summary", {}) or {}

    if consistency.get("status") not in {"", "ok"} or not consistency.get("ok", True):
        reason_codes.append("TRADE_PAPER_RECONCILE_DRIFT")

    for rule_code in advisory_summary.get("triggered_rules", []) or []:
        normalized = normalize_reason_code(rule_code)
        if normalized in {"RISK_TIME_STOP", "RISK_DRAWDOWN_TAKE_PROFIT"}:
            reason_codes.append(normalized)
        elif normalized:
            reason_codes.append(normalized)

    reason_codes = _prioritize_codes(
        reason_codes,
        [
            "TRADE_PAPER_RECONCILE_DRIFT",
            "TRADE_PORTFOLIO_DAILY_LOSS_LIMIT",
            "TRADE_CONSECUTIVE_LOSS_COOLDOWN",
            "TRADE_WEEKLY_BUY_LIMIT",
            "TRADE_EXPOSURE_LIMIT",
            "TRADE_HOLDING_LIMIT",
            "TRADE_POSITION_CONCENTRATION_WARNING",
            "RISK_TIME_STOP",
            "RISK_DRAWDOWN_TAKE_PROFIT",
            "TRADE_ADVISORY",
            "TRADE_OK",
        ],
    )
    if not reason_codes:
        reason_codes = ["TRADE_OK"]

    state = "ok"
    if "TRADE_PAPER_RECONCILE_DRIFT" in reason_codes:
        state = "drift"
    elif any(
        code in {
            "TRADE_PORTFOLIO_DAILY_LOSS_LIMIT",
            "TRADE_CONSECUTIVE_LOSS_COOLDOWN",
            "TRADE_WEEKLY_BUY_LIMIT",
            "TRADE_EXPOSURE_LIMIT",
            "TRADE_HOLDING_LIMIT",
        }
        for code in reason_codes
    ):
        state = "block"
    elif any(
        code in {
            "TRADE_POSITION_CONCENTRATION_WARNING",
            "RISK_TIME_STOP",
            "RISK_DRAWDOWN_TAKE_PROFIT",
            "TRADE_ADVISORY",
        }
        for code in reason_codes
    ):
        state = "warning"

    return summarize_reason_codes(
        "trade",
        reason_codes,
        source_codes=source_codes + [str(rule).strip() for rule in advisory_summary.get("triggered_rules", []) or []],
        state=state,
        details={
            "decision": today_decision.get("decision", ""),
            "market_signal": today_decision.get("market_signal", ""),
            "risk": today_decision.get("risk", {}),
            "portfolio_risk": today_decision.get("portfolio_risk", {}),
            "shadow_status": shadow_snapshot.get("status", ""),
            "consistency_status": consistency.get("status", ""),
            "triggered_signal_count": advisory_summary.get("triggered_signal_count", 0),
            "triggered_position_count": advisory_summary.get("triggered_position_count", 0),
        },
    )


def build_signal_bus_summary(market_snapshot: dict | None = None,
                             pool_snapshot: dict | None = None,
                             pool_audit: dict | None = None,
                             today_decision: dict | None = None,
                             shadow_snapshot: dict | None = None) -> dict:
    market = _market_summary(market_snapshot)
    pool = _pool_summary(pool_snapshot, pool_audit=pool_audit)
    trade = _trade_summary(today_decision, shadow_snapshot)

    overall_state = "ok"
    if any(item["state"] == "drift" for item in (pool, trade)):
        overall_state = "drift"
    elif any(item["state"] in {"warning", "block"} for item in (pool, trade)):
        overall_state = "warning"
    if market["state"] in {"RED", "CLEAR"} and overall_state == "ok":
        overall_state = "warning" if market["state"] == "RED" else overall_state

    return {
        "version": 1,
        "state": overall_state,
        "market": market,
        "pool": pool,
        "trade": trade,
    }
