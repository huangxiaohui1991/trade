#!/usr/bin/env python3
"""
统一 CLI 入口

支持：
  trade doctor
  trade run <pipeline>
  trade status today
"""

import argparse
import contextlib
import io
import json
import logging
import os
import platform
import sys
import tempfile
import traceback
from datetime import datetime
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.engine.composite import build_today_decision
from scripts.backtest import compare_backtest_history, list_backtest_history, run_backtest, run_parameter_sweep, run_walk_forward
from scripts.mx.cli_tools import MXCommandError, dispatch_mx_command, list_mx_command_metadata, mx_command_groups
from scripts.pipeline.core_pool_scoring import run as run_scoring
from scripts.pipeline.evening import run as run_evening
from scripts.pipeline.morning import run as run_morning
from scripts.pipeline.noon import run as run_noon
from scripts.pipeline.stock_screener import run as run_screener
from scripts.pipeline.weekly_review import run as run_weekly
from scripts.state.reason_codes import build_signal_bus_summary
from scripts.state import (
    AUTOMATED_RULES,
    LEDGER_DB_PATH,
    apply_order_reply,
    audit_state,
    bootstrap_state,
    load_market_snapshot,
    load_alert_snapshot,
    load_pool_action_history,
    load_order_snapshot,
    load_pool_snapshot,
    load_portfolio_snapshot,
    load_trade_review,
    pending_condition_order_items,
    upsert_order_state,
    sync_activity_state,
    sync_portfolio_state,
)
from scripts.utils.cache import CACHE_DIR
from scripts.utils.config_loader import get_notification, get_strategy
from scripts.utils.discord_push import render_condition_order_reminder, send_condition_order_reminder
from scripts.utils.obsidian import ObsidianVault
from scripts.utils.run_context import (
    LOCK_DIR,
    RUNS_DIR,
    now_ts,
    make_run_id,
    pipeline_lock,
    sanitize_for_json,
    sync_run_to_daily_state,
    write_run_result,
)
from scripts.utils.runtime_state import RUNTIME_DIR, load_daily_state
from scripts.utils.logger import set_console_logging


PIPELINES = {
    "morning": lambda args: run_morning(),
    "noon": lambda args: run_noon(),
    "evening": lambda args: run_evening(),
    "scoring": lambda args: run_scoring(),
    "weekly": lambda args: run_weekly(),
    "screener": lambda args: run_screener(pool=args.pool, universe=args.universe),
    "sentiment": lambda args: _run_sentiment(args),
    "hk_monitor": lambda args: _run_hk_monitor(args),
    "monthly": lambda args: _run_monthly(args),
}


def _run_sentiment(args):
    from scripts.pipeline.sentiment_monitor import run as run_sentiment
    return run_sentiment(dry_run=getattr(args, "dry_run", False))


def _run_hk_monitor(args):
    from scripts.pipeline.hk_monitor import run as run_hk_monitor
    return run_hk_monitor(dry_run=getattr(args, "dry_run", False))


def _run_monthly(args):
    from scripts.pipeline.monthly_review import run as run_monthly
    return run_monthly(month=getattr(args, "month", None))

PIPELINE_ALIASES = {
    "stock_screener": "screener",
}

DATA_HEALTH_CACHE_TTL_SECONDS = {
    "financial": 7 * 24 * 3600,
    "flow": 24 * 3600,
    "market_timer": 24 * 3600,
    "screening_candidates": 2 * 24 * 3600,
    "trading_calendar": 7 * 24 * 3600,
}

DATA_HEALTH_PIPELINES = {
    "morning",
    "noon",
    "evening",
    "scoring",
    "screener",
    "sentiment",
    "hk_monitor",
}

WORKFLOWS = {
    "morning_brief": {
        "steps": ["status", "morning"],
        "preferred_for": ["Hermes-Agent", "OpenClaw"],
        "timeout_seconds": 90,
        "retryable_steps": ["morning"],
        "fallback_workflow": None,
        "notes": "盘前摘要，优先读取今日状态再执行盘前流程。",
    },
    "noon_check": {
        "steps": ["status", "noon"],
        "preferred_for": ["Hermes-Agent", "OpenClaw"],
        "timeout_seconds": 90,
        "retryable_steps": ["noon"],
        "fallback_workflow": None,
        "notes": "午休检查，适合定时巡检和会话内补跑。",
    },
    "close_review": {
        "steps": ["status", "evening", "scoring"],
        "preferred_for": ["Hermes-Agent", "OpenClaw"],
        "timeout_seconds": 180,
        "retryable_steps": ["evening", "scoring"],
        "fallback_workflow": "tracked_scan",
        "notes": "收盘更新 + 核心池评分，适合作为日终主工作流。",
    },
    "weekly_review": {
        "steps": ["status", "weekly"],
        "preferred_for": ["Hermes-Agent", "OpenClaw"],
        "timeout_seconds": 120,
        "retryable_steps": ["weekly"],
        "fallback_workflow": None,
        "notes": "周报汇总，外层可直接消费 artifacts 生成摘要。",
    },
    "tracked_scan": {
        "steps": ["status", "screener"],
        "preferred_for": ["Hermes-Agent", "OpenClaw"],
        "timeout_seconds": 240,
        "retryable_steps": ["screener"],
        "fallback_workflow": None,
        "notes": "已跟踪池扫描，稳定性高于全市场模式。",
        "default_args": {"pool": "all", "universe": "tracked"},
    },
    "market_scan": {
        "steps": ["status", "screener"],
        "preferred_for": ["Hermes-Agent", "OpenClaw"],
        "timeout_seconds": 360,
        "retryable_steps": ["screener"],
        "fallback_workflow": "tracked_scan",
        "notes": "全市场扫描，依赖外部接口，建议外层保留超时和重试。",
        "default_args": {"pool": "all", "universe": "market"},
    },
}

AGENT_TEMPLATES = {
    "success": {
        "Hermes-Agent": "流程已完成。优先汇总 artifacts、today_decision 和 pool_management，再决定是否继续后续 workflow。",
        "OpenClaw": "流程执行成功。结合 status_after.today_decision、artifacts 和 steps，生成面向用户的简要结论。",
    },
    "warning": {
        "Hermes-Agent": "流程完成但存在降级或依赖问题。继续汇报结果，同时明确 warning 原因，不自动中止。",
        "OpenClaw": "结果可用但存在风险。展示核心结论时附带 warning，并优先引用 artifacts 而不是原始 result。",
    },
    "blocked": {
        "Hermes-Agent": "流程被 doctor 或运行锁阻断。停止后续步骤，汇报 blocked 原因；若 workflow 有 fallback_workflow，可建议切换。",
        "OpenClaw": "流程被阻断。不要继续手拼 pipeline，优先提示原因；若存在 fallback_workflow，可征得用户同意后切换。",
    },
    "error": {
        "Hermes-Agent": "流程失败。停止后续执行，汇报 failed_step、error 和是否 retryable。",
        "OpenClaw": "流程失败。向用户解释失败步骤和原因；只有在 retryable=true 时才建议重试。",
    },
}


def _json_print(payload: dict):
    print(json.dumps(sanitize_for_json(payload), ensure_ascii=False, indent=2))


def order_command(action: str, args) -> dict:
    """Handle `trade order <action>` subcommands for Hermes-Agent."""
    if action == "confirm":
        result = apply_order_reply(
            reply_text=getattr(args, "reply", ""),
            scope=getattr(args, "scope", "paper_mx"),
        )
        return sanitize_for_json({
            "command": "order",
            "action": "confirm",
            **result,
        })

    if action == "pending":
        scope = getattr(args, "scope", "paper_mx")
        pending = pending_condition_order_items(scope=scope)
        return sanitize_for_json({
            "command": "order",
            "action": "pending",
            "status": "ok",
            "scope": scope,
            "pending_count": len(pending),
            "items": pending,
        })

    if action == "remind":
        scope = getattr(args, "scope", "paper_mx")
        pending = pending_condition_order_items(scope=scope)
        content = render_condition_order_reminder(pending)
        send = bool(getattr(args, "send", False))
        discord_ok = False
        discord_error = ""
        if send:
            discord_ok, discord_error = send_condition_order_reminder(pending)
        return sanitize_for_json({
            "command": "order",
            "action": "remind",
            "status": "ok" if (not send or discord_ok) else "warning",
            "scope": scope,
            "pending_count": len(pending),
            "items": pending,
            "send": send,
            "discord_ok": discord_ok,
            "discord_error": discord_error,
            "content": content,
        })

    if action == "overdue-check":
        scope = getattr(args, "scope", "paper_mx")
        send = bool(getattr(args, "send", False))
        result = _check_overdue_orders(scope=scope, send=send)
        return sanitize_for_json({
            "command": "order",
            "action": "overdue-check",
            **result,
        })

    if action == "place":
        now_ts_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        scope = getattr(args, "scope", "paper_mx")
        code = getattr(args, "code", "")
        name = getattr(args, "name", "")
        side = getattr(args, "side", "sell")
        condition_type = getattr(args, "condition_type", "manual_stop")
        price = getattr(args, "price", 0.0)
        shares = getattr(args, "shares", 0)
        reason = getattr(args, "reason", "")

        order = upsert_order_state({
            "external_id": f"{scope}:agent:{datetime.now().strftime('%Y%m%d%H%M%S%f')}:{code}:{condition_type}",
            "scope": scope,
            "broker": "hermes_agent",
            "code": code,
            "name": name,
            "side": side,
            "order_class": "condition",
            "order_type": "conditional",
            "condition_type": condition_type,
            "requested_shares": shares,
            "filled_shares": 0,
            "trigger_price": price,
            "status": "placed",
            "confirm_status": "not_required",
            "reason_code": f"AGENT_{condition_type.upper()}",
            "reason_text": reason or f"Hermes-Agent placed {condition_type}",
            "source": "hermes_agent",
            "placed_at": now_ts_str,
            "updated_at": now_ts_str,
            "metadata": {"placed_by": "hermes_agent"},
        })
        return sanitize_for_json({
            "command": "order",
            "action": "place",
            "status": "ok",
            "order": order,
        })

    if action == "cancel":
        scope = getattr(args, "scope", "paper_mx")
        code = getattr(args, "code", "")
        name = getattr(args, "name", "")
        condition_type = getattr(args, "condition_type", "")
        now_ts_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        # Find matching open orders
        snapshot = load_order_snapshot(scope=scope)
        candidates = []
        for order in snapshot.get("orders", []):
            if str(order.get("code", "")).strip() != code:
                continue
            if order.get("status") in {"filled", "cancelled", "reviewed", "exception"}:
                continue
            if condition_type and str(order.get("condition_type", "")).strip() != condition_type:
                continue
            candidates.append(order)

        cancelled = []
        for order in candidates:
            updated = upsert_order_state({
                "external_id": order["external_id"],
                "status": "cancelled",
                "cancelled_at": now_ts_str,
                "updated_at": now_ts_str,
                "source": "hermes_agent",
                "metadata": {
                    **(order.get("metadata", {}) if isinstance(order.get("metadata", {}), dict) else {}),
                    "cancelled_by": "hermes_agent",
                },
            })
            cancelled.append(updated)

        return sanitize_for_json({
            "command": "order",
            "action": "cancel",
            "status": "ok",
            "code": code,
            "cancelled_count": len(cancelled),
            "cancelled_orders": cancelled,
        })

    if action == "modify":
        scope = getattr(args, "scope", "paper_mx")
        code = getattr(args, "code", "")
        new_price = getattr(args, "price", 0.0)
        condition_type = getattr(args, "condition_type", "")
        now_ts_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        snapshot = load_order_snapshot(scope=scope)
        candidates = []
        for order in snapshot.get("orders", []):
            if str(order.get("code", "")).strip() != code:
                continue
            if order.get("status") in {"filled", "cancelled", "reviewed", "exception"}:
                continue
            if condition_type and str(order.get("condition_type", "")).strip() != condition_type:
                continue
            candidates.append(order)

        modified = []
        for order in candidates:
            old_price = float(order.get("trigger_price", 0) or 0)
            updated = upsert_order_state({
                "external_id": order["external_id"],
                "trigger_price": new_price,
                "status": "placed",
                "updated_at": now_ts_str,
                "source": "hermes_agent",
                "metadata": {
                    **(order.get("metadata", {}) if isinstance(order.get("metadata", {}), dict) else {}),
                    "modified_by": "hermes_agent",
                    "old_trigger_price": old_price,
                },
            })
            modified.append(updated)

        return sanitize_for_json({
            "command": "order",
            "action": "modify",
            "status": "ok",
            "code": code,
            "new_price": new_price,
            "modified_count": len(modified),
            "modified_orders": modified,
        })

    if action == "list":
        snapshot = load_order_snapshot(
            scope=getattr(args, "scope", None),
            status=getattr(args, "status", None),
        )
        limit = getattr(args, "limit", 20)
        orders = snapshot.get("orders", [])[:limit]
        return sanitize_for_json({
            "command": "order",
            "action": "list",
            "status": "ok",
            "order_count": len(orders),
            "summary": snapshot.get("summary", {}),
            "orders": orders,
        })

    return {"command": "order", "action": action, "status": "error", "error": f"unknown action: {action}"}


def _check_overdue_orders(scope: str = "paper_mx", send: bool = False) -> dict:
    """
    检查超时未确认的条件单。

    规则：
      - T+1 9:15 未确认 → 再提醒一次
      - T+2 15:00 未确认 → 标记异常
    """
    now = datetime.now()
    snapshot = load_order_snapshot(scope=scope)
    orders = snapshot.get("orders", [])

    t1_remind = []   # 需要再提醒
    t2_exception = []  # 需要标记异常

    for order in orders:
        status = str(order.get("status", "")).strip()
        confirm_status = str(order.get("confirm_status", "")).strip()

        # 只检查 placed/partially_filled 且需要确认的
        if status not in {"placed", "partially_filled", "triggered"}:
            continue
        if confirm_status in {"confirmed", "not_required"}:
            continue

        placed_at = str(order.get("placed_at", "")).strip()
        if not placed_at:
            continue

        try:
            placed_time = datetime.strptime(placed_at[:19], "%Y-%m-%dT%H:%M:%S")
        except (ValueError, TypeError):
            continue

        hours_since = (now - placed_time).total_seconds() / 3600

        if hours_since >= 48:  # T+2
            t2_exception.append(order)
        elif hours_since >= 24:  # T+1
            t1_remind.append(order)

    # 处理 T+2 异常标记
    for order in t2_exception:
        upsert_order_state({
            "external_id": order["external_id"],
            "status": "exception",
            "confirm_status": "overdue_exception",
            "updated_at": now.strftime("%Y-%m-%dT%H:%M:%S"),
            "source": "overdue_check",
            "metadata": {
                **(order.get("metadata", {}) if isinstance(order.get("metadata", {}), dict) else {}),
                "overdue_marked_at": now.strftime("%Y-%m-%dT%H:%M:%S"),
                "overdue_reason": "T+2 未确认",
            },
        })

    # 处理 T+1 提醒
    discord_ok = False
    discord_error = ""
    if t1_remind and send:
        pending_items = []
        for order in t1_remind:
            condition_type = str(order.get("condition_type", "")).strip()
            order_type = "止盈" if "profit" in condition_type or condition_type.endswith("_tp") else "止损"
            pending_items.append({
                "name": order.get("name", ""),
                "type": f"{order_type}（超时提醒）",
                "price": float(order.get("trigger_price", 0) or 0),
                "currency": "¥",
                "status": "T+1 未确认",
            })
        discord_ok, discord_error = send_condition_order_reminder(pending_items)

    return {
        "status": "ok" if not t2_exception else "warning",
        "scope": scope,
        "t1_remind_count": len(t1_remind),
        "t2_exception_count": len(t2_exception),
        "t1_remind_orders": [
            {"external_id": o.get("external_id", ""), "code": o.get("code", ""), "name": o.get("name", "")}
            for o in t1_remind
        ],
        "t2_exception_orders": [
            {"external_id": o.get("external_id", ""), "code": o.get("code", ""), "name": o.get("name", "")}
            for o in t2_exception
        ],
        "send": send,
        "discord_ok": discord_ok,
        "discord_error": discord_error,
    }


def list_workflows() -> dict:
    return {
        "command": "workflows",
        "items": [
            {
                "name": name,
                **spec,
            }
            for name, spec in WORKFLOWS.items()
        ],
    }


def list_agent_templates() -> dict:
    return {
        "command": "templates",
        "items": AGENT_TEMPLATES,
    }


def _recommend_next_actions(status: str, workflow_name: str | None = None, error: str = "", retryable: bool = False) -> list:
    workflow = WORKFLOWS.get(workflow_name or "", {})
    actions = []

    if status == "success":
        actions.append("读取 status_after.today_decision")
        actions.append("读取 artifacts 生成摘要")
    elif status == "warning":
        actions.append("继续消费 artifacts 和 status_after")
        actions.append("在输出里附带 doctor.warning 或步骤 warning")
    elif status == "blocked":
        actions.append("停止后续 workflow")
        if workflow.get("fallback_workflow"):
            actions.append(f"可改跑 orchestrate {workflow['fallback_workflow']}")
        if retryable:
            actions.append("等待 30-60 秒后重试 1 次")
    elif status == "error":
        actions.append("停止后续 workflow")
        actions.append("汇报 failed_step 和 error")
        if retryable:
            actions.append("可人工触发重试 1 次")

    if workflow_name == "market_scan" and status in {"warning", "blocked", "error"}:
        actions.append("优先降级到 tracked_scan")
    if error == "doctor_failed":
        actions.append("先处理 doctor.hard_fail，再重试")

    deduped = []
    seen = set()
    for item in actions:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _check_path_writable(path: Path) -> dict:
    try:
        path.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path, prefix=".write_test_", delete=False) as f:
            f.write("ok")
            probe = Path(f.name)
        probe.unlink(missing_ok=True)
        return {"ok": True, "path": str(path)}
    except Exception as e:
        return {"ok": False, "path": str(path), "error": str(e)}


def _requests_ok(url: str, timeout: int = 8) -> dict:
    try:
        resp = requests.get(url, timeout=timeout)
        return {"ok": True, "status_code": resp.status_code}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _parse_health_ts(value) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    for candidate in (text, text[:19]):
        try:
            parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
            if parsed.tzinfo is not None:
                parsed = parsed.replace(tzinfo=None)
            return parsed
        except ValueError:
            continue
    return None


def _load_json_file(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _financial_missing_fields(data) -> list[str]:
    if not isinstance(data, dict):
        return []
    missing = []
    if data.get("roe") is None and not data.get("roe_recent"):
        missing.append("ROE")
    if data.get("revenue_growth") is None:
        missing.append("营收")
    if data.get("operating_cash_flow") is None and data.get("cash_flow_positive") is None:
        missing.append("现金流")
    return missing


def _cache_namespace_health(namespace: str, max_age_seconds: int, *, cache_dir: Path, now: datetime) -> dict:
    ns_dir = cache_dir / namespace
    files = sorted(ns_dir.glob("*.json"), key=lambda path: path.stat().st_mtime, reverse=True) if ns_dir.exists() else []
    summary = {
        "status": "unknown",
        "file_count": len(files),
        "fresh_count": 0,
        "stale_count": 0,
        "error_count": 0,
        "missing_field_count": 0,
        "missing_field_rate": 0.0,
        "stale_rate": 0.0,
        "last_success_at": "",
        "max_age_seconds": max_age_seconds,
        "sample": [],
    }
    if not files:
        return summary

    newest_success: datetime | None = None
    for path in files[:50]:
        payload = _load_json_file(path)
        if not isinstance(payload, dict):
            summary["error_count"] += 1
            continue
        data = payload.get("data")
        cached_at = _parse_health_ts(payload.get("cached_at"))
        age_seconds = round((now - cached_at).total_seconds(), 1) if cached_at else None
        data_error = isinstance(data, dict) and bool(data.get("error"))
        data_stale = isinstance(data, dict) and bool(data.get("stale"))
        cache_stale = age_seconds is None or age_seconds > max_age_seconds or data_stale
        missing_fields = _financial_missing_fields(data) if namespace == "financial" else []

        if data_error:
            summary["error_count"] += 1
        if cache_stale:
            summary["stale_count"] += 1
        else:
            summary["fresh_count"] += 1
        if missing_fields:
            summary["missing_field_count"] += 1
        if cached_at and not data_error:
            if newest_success is None or cached_at > newest_success:
                newest_success = cached_at
        if len(summary["sample"]) < 5:
            summary["sample"].append({
                "key": path.stem,
                "cached_at": payload.get("cached_at", ""),
                "age_seconds": age_seconds,
                "stale": bool(cache_stale),
                "error": bool(data_error),
                "missing_fields": missing_fields,
                "source": (payload.get("meta", {}) or {}).get("source", "") if isinstance(payload.get("meta", {}), dict) else "",
            })

    if newest_success:
        summary["last_success_at"] = newest_success.strftime("%Y-%m-%dT%H:%M:%S")
    if files:
        summary["missing_field_rate"] = round(summary["missing_field_count"] / len(files), 4)
        summary["stale_rate"] = round(summary["stale_count"] / len(files), 4)

    if summary["error_count"] > 0 or summary["stale_rate"] > 0.5:
        summary["status"] = "warning"
    elif summary["missing_field_count"] > 0:
        summary["status"] = "warning"
    else:
        summary["status"] = "ok"
    return summary


def _recent_run_health(*, runs_dir: Path, recent_limit: int) -> dict:
    files = []
    if runs_dir.exists():
        files = sorted(runs_dir.glob("*/*.json"), key=lambda path: path.stat().st_mtime, reverse=True)

    runs = []
    for path in files:
        payload = _load_json_file(path)
        if not isinstance(payload, dict):
            continue
        pipeline = str(payload.get("pipeline", "")).strip()
        if pipeline not in DATA_HEALTH_PIPELINES:
            continue
        result = payload.get("result", {})
        result_status = str(result.get("status", "")).strip().lower() if isinstance(result, dict) else ""
        status = result_status or str(payload.get("status", "")).strip().lower()
        normalized_status = "success" if status in {"ok", "success"} else status
        runs.append({
            "pipeline": pipeline,
            "run_id": payload.get("run_id", ""),
            "status": normalized_status,
            "started_at": payload.get("started_at", ""),
            "finished_at": payload.get("finished_at", ""),
            "result_path": str(path),
        })
        if len(runs) >= recent_limit:
            break

    counted = [item for item in runs if item["status"] not in {"skipped"}]
    success_count = sum(1 for item in counted if item["status"] == "success")
    warning_count = sum(1 for item in counted if item["status"] == "warning")
    failure_count = sum(1 for item in counted if item["status"] in {"error", "blocked"})
    usable_count = success_count + warning_count
    denominator = len(counted)
    usable_rate = round(usable_count / denominator, 4) if denominator else 0.0
    success_rate = round(success_count / denominator, 4) if denominator else 0.0
    last_success_at = ""
    for item in runs:
        if item["status"] == "success":
            last_success_at = str(item.get("finished_at") or item.get("started_at") or "")
            break

    if not runs:
        status = "unknown"
    elif failure_count > 0 or usable_rate < 0.8:
        status = "warning"
    else:
        status = "ok"
    return {
        "status": status,
        "recent_limit": recent_limit,
        "run_count": len(runs),
        "counted_run_count": denominator,
        "success_count": success_count,
        "warning_count": warning_count,
        "failure_count": failure_count,
        "usable_rate": usable_rate,
        "success_rate": success_rate,
        "last_success_at": last_success_at,
        "sample": runs[:5],
    }


def _score_data_quality_health(pool_snapshot: dict | None = None) -> dict:
    try:
        snapshot = pool_snapshot if pool_snapshot is not None else load_pool_snapshot()
        entries = snapshot.get("entries", []) if isinstance(snapshot, dict) else []
    except Exception as exc:
        return {"status": "warning", "entry_count": 0, "error": str(exc)}

    counts = {"ok": 0, "degraded": 0, "error": 0, "unknown": 0}
    missing_field_count = 0
    for entry in entries:
        quality = str(entry.get("data_quality", "ok") or "ok").strip().lower()
        if quality not in counts:
            quality = "unknown"
        counts[quality] += 1
        missing = entry.get("data_missing_fields", [])
        if isinstance(missing, str):
            has_missing = bool(missing.strip())
        else:
            has_missing = bool(missing)
        if has_missing:
            missing_field_count += 1

    entry_count = len(entries)
    status = "unknown"
    if entry_count:
        status = "warning" if counts["degraded"] or counts["error"] or counts["unknown"] else "ok"
    return {
        "status": status,
        "entry_count": entry_count,
        "quality_counts": counts,
        "missing_field_count": missing_field_count,
        "missing_field_rate": round(missing_field_count / entry_count, 4) if entry_count else 0.0,
    }


def _data_source_health_snapshot(
    *,
    cache_dir: Path | None = None,
    runs_dir: Path | None = None,
    recent_limit: int = 20,
    now: datetime | None = None,
    pool_snapshot: dict | None = None,
) -> dict:
    cache_root = Path(cache_dir or CACHE_DIR)
    runs_root = Path(runs_dir or RUNS_DIR)
    current_time = now or datetime.now()

    cache_namespaces = {
        name: _cache_namespace_health(name, ttl, cache_dir=cache_root, now=current_time)
        for name, ttl in DATA_HEALTH_CACHE_TTL_SECONDS.items()
    }
    recent_runs = _recent_run_health(runs_dir=runs_root, recent_limit=recent_limit)
    score_quality = _score_data_quality_health(pool_snapshot=pool_snapshot)

    cache_file_count = sum(item.get("file_count", 0) for item in cache_namespaces.values())
    cache_warning_count = sum(1 for item in cache_namespaces.values() if item.get("status") == "warning")
    cache_unknown_count = sum(1 for item in cache_namespaces.values() if item.get("status") == "unknown")
    warnings = []
    if recent_runs.get("status") == "warning":
        warnings.append("recent_pipeline_runs")
    if cache_warning_count:
        warnings.append("cache_freshness")
    if score_quality.get("status") == "warning":
        warnings.append("score_data_quality")

    has_observation = bool(cache_file_count or recent_runs.get("run_count") or score_quality.get("entry_count"))
    if warnings:
        status = "warning"
    elif has_observation:
        status = "ok"
    else:
        status = "unknown"

    latest_success_values = [
        recent_runs.get("last_success_at", ""),
        *[item.get("last_success_at", "") for item in cache_namespaces.values()],
    ]
    latest_success_values = [item for item in latest_success_values if item]
    latest_success_at = max(latest_success_values) if latest_success_values else ""
    return {
        "ok": status == "ok",
        "status": status,
        "warning": warnings,
        "source": "runs+cache+pool_snapshot",
        "recent_runs": recent_runs,
        "cache_summary": {
            "namespace_count": len(cache_namespaces),
            "warning_namespace_count": cache_warning_count,
            "unknown_namespace_count": cache_unknown_count,
            "file_count": cache_file_count,
        },
        "cache_namespaces": cache_namespaces,
        "score_data_quality": score_quality,
        "last_success_at": latest_success_at,
    }


def _mx_health_snapshot(include_unavailable: bool = False) -> dict:
    items = list_mx_command_metadata(include_unavailable=include_unavailable)
    groups = mx_command_groups(include_unavailable=include_unavailable)
    unavailable_items = [item for item in items if not item.get("available", False)]
    available_items = [item for item in items if item.get("available", False)]
    required_commands = [
        "mx.data.query",
        "mx.search.news",
        "mx.xuangu.search",
        "mx.zixuan.query",
        "mx.zixuan.manage",
        "mx.moni.positions",
        "mx.moni.balance",
        "mx.moni.orders",
        "mx.moni.buy",
        "mx.moni.sell",
        "mx.moni.cancel",
        "mx.moni.cancel_all",
    ]
    item_lookup = {item.get("id", ""): item for item in items}
    required = {
        command_id: {
            "available": bool(item_lookup.get(command_id, {}).get("available", False)),
            "availability_note": item_lookup.get(command_id, {}).get("availability_note", ""),
        }
        for command_id in required_commands
    }
    return {
        "status": "ok" if not unavailable_items else "warning",
        "available_count": len(available_items),
        "unavailable_count": len(unavailable_items),
        "command_count": len(items),
        "group_count": len(groups),
        "groups": {name: len(values) for name, values in groups.items()},
        "required": required,
        "unavailable_commands": [item.get("id", "") for item in unavailable_items],
        "source": "scripts.mx.cli_tools",
    }


def _shadow_trade_snapshot() -> dict:
    empty_advisory = {
        "triggered_signal_count": 0,
        "triggered_position_count": 0,
        "triggered_rules": [],
        "positions": [],
    }
    try:
        from scripts.pipeline.shadow_trade import get_status, paper_trade_consistency_snapshot

        consistency = paper_trade_consistency_snapshot(window=180)
        shadow_status = get_status()
        actual_positions = [
            {
                "code": str(item.get("code", "")).strip(),
                "name": str(item.get("name", "")).strip(),
                "shares": int(float(item.get("shares", 0) or 0)),
            }
            for item in shadow_status.get("positions", [])
            if str(item.get("code", "")).strip() and int(float(item.get("shares", 0) or 0)) > 0
        ]
        return {
            "ok": True,
            "status": consistency["status"],
            "timestamp": shadow_status.get("timestamp", ""),
            "automation_scope": shadow_status.get("automation_scope", ""),
            "automated_rules": shadow_status.get("automated_rules", []),
            "advisory_rules": shadow_status.get("advisory_rules", []),
            "mx_health": shadow_status.get("mx_health", _mx_health_snapshot(include_unavailable=True)),
            "positions_count": len(actual_positions),
            "positions": actual_positions,
            "advisory_summary": shadow_status.get("advisory_summary", empty_advisory),
            "consistency": consistency,
        }
    except Exception as e:
        return {
            "ok": False,
            "status": "error",
            "error": str(e),
            "timestamp": "",
            "automation_scope": "",
            "automated_rules": [],
            "advisory_rules": [],
            "mx_health": _mx_health_snapshot(include_unavailable=True),
            "positions_count": 0,
            "positions": [],
            "advisory_summary": empty_advisory,
            "consistency": {
                "ok": False,
                "status": "error",
                "error": str(e),
                "inferred_open_codes": [],
                "actual_open_codes": [],
                "event_only_codes": [],
                "broker_only_codes": [],
                "event_trade_count": 0,
            },
        }


def _order_count_from_statuses(status_counts: dict, statuses: list[str]) -> int:
    total = 0
    for status in statuses:
        total += int(status_counts.get(status, 0) or 0)
    return total


def _compact_order_snapshot(order_snapshot: dict, *, sample_size: int = 3) -> dict:
    orders = order_snapshot.get("orders", []) if isinstance(order_snapshot, dict) else []
    summary = dict(order_snapshot.get("summary", {}) if isinstance(order_snapshot, dict) else {})
    status_counts = dict(summary.get("status_counts", {}) or {})

    pending_count = _order_count_from_statuses(status_counts, ["candidate", "pending", "confirm_pending"])
    open_count = _order_count_from_statuses(status_counts, ["placed", "partially_filled", "cancel_requested", "triggered"])
    exception_count = _order_count_from_statuses(status_counts, ["exception", "rejected", "failed", "cancel_failed"])
    review_queue_count = _order_count_from_statuses(status_counts, ["review_required", "review_pending"])
    partial_fill_count = _order_count_from_statuses(status_counts, ["partially_filled"])
    cancel_replace_count = _order_count_from_statuses(status_counts, ["cancel_replace_pending"])

    condition_orders = []
    for order in orders:
        order_class = str(order.get("order_class", "")).strip()
        condition_type = str(order.get("condition_type", "")).strip()
        if order_class == "condition" or condition_type:
            condition_orders.append(order)

    condition_status_counts: dict[str, int] = {}
    condition_type_counts: dict[str, int] = {}
    for order in condition_orders:
        status = str(order.get("status", "")).strip() or "unknown"
        condition_status_counts[status] = condition_status_counts.get(status, 0) + 1
        condition_type = str(order.get("condition_type", "")).strip() or "unknown"
        condition_type_counts[condition_type] = condition_type_counts.get(condition_type, 0) + 1

    compact_condition_orders = {
        "count": len(condition_orders),
        "pending_count": _order_count_from_statuses(condition_status_counts, ["candidate", "pending", "confirm_pending"]),
        "open_count": _order_count_from_statuses(condition_status_counts, ["placed", "partially_filled", "cancel_requested", "triggered"]),
        "exception_count": _order_count_from_statuses(condition_status_counts, ["exception", "rejected", "failed", "cancel_failed"]),
        "review_queue_count": _order_count_from_statuses(condition_status_counts, ["review_required", "review_pending"]),
        "partial_fill_count": _order_count_from_statuses(condition_status_counts, ["partially_filled"]),
        "cancel_replace_count": _order_count_from_statuses(condition_status_counts, ["cancel_replace_pending"]),
        "status_counts": condition_status_counts,
        "condition_type_counts": condition_type_counts,
        "sample": [],
    }
    for order in condition_orders[:sample_size]:
        compact_condition_orders["sample"].append({
            "external_id": order.get("external_id", ""),
            "code": order.get("code", ""),
            "name": order.get("name", ""),
            "side": order.get("side", ""),
            "status": order.get("status", ""),
            "condition_type": order.get("condition_type", ""),
            "requested_shares": order.get("requested_shares", 0),
            "filled_shares": order.get("filled_shares", 0),
            "trigger_price": order.get("trigger_price", 0.0),
            "limit_price": order.get("limit_price", 0.0),
            "confirm_status": order.get("confirm_status", ""),
        })

    return {
        "scope": order_snapshot.get("scope", "all") if isinstance(order_snapshot, dict) else "all",
        "status": order_snapshot.get("status", "all") if isinstance(order_snapshot, dict) else "all",
        "db_path": order_snapshot.get("db_path", str(LEDGER_DB_PATH)) if isinstance(order_snapshot, dict) else str(LEDGER_DB_PATH),
        "summary": {
            **summary,
            "pending_count": pending_count,
            "open_count": open_count,
            "exception_count": exception_count,
            "review_queue_count": summary.get("review_queue_count", review_queue_count),
            "partial_fill_count": summary.get("partial_fill_count", partial_fill_count),
            "cancel_replace_count": summary.get("cancel_replace_count", cancel_replace_count),
        },
        "condition_orders": compact_condition_orders,
    }


def _build_alert_snapshot(today_decision: dict, pool_sync_state: dict, shadow_snapshot: dict,
                          order_snapshot: dict, signal_bus: dict, pool_snapshot: dict | None = None) -> dict:
    alerts = []

    def add_alert(level: str, code: str, summary: str, details: dict | None = None):
        alerts.append({
            "level": level,
            "code": code,
            "summary": summary,
            "details": details or {},
        })

    if pool_sync_state.get("status") not in {"", "ok"}:
        add_alert("warning", "POOL_SYNC_DRIFT", "池子投影存在漂移", {
            "status": pool_sync_state.get("status", ""),
            "snapshot_date": pool_sync_state.get("snapshot_date", ""),
        })

    consistency = shadow_snapshot.get("consistency", {}) or {}
    if consistency.get("status") not in {"", "ok"} or not consistency.get("ok", True):
        add_alert("warning", "TRADE_PAPER_RECONCILE_DRIFT", "模拟盘事件流与 broker 状态不一致", {
            "event_only_codes": consistency.get("event_only_codes", []),
            "broker_only_codes": consistency.get("broker_only_codes", []),
        })

    order_summary = order_snapshot.get("summary", {}) if isinstance(order_snapshot, dict) else {}
    if int(order_summary.get("pending_count", 0) or 0) > 0:
        add_alert("info", "ORDER_CONFIRM_PENDING", "存在待确认条件单", {
            "pending_count": order_summary.get("pending_count", 0),
            "condition_orders": order_snapshot.get("condition_orders", {}),
        })
    if int(order_summary.get("exception_count", 0) or 0) > 0:
        add_alert("warning", "ORDER_EXCEPTION", "存在异常订单", {
            "exception_count": order_summary.get("exception_count", 0),
        })

    portfolio_risk = today_decision.get("portfolio_risk", {}) if isinstance(today_decision, dict) else {}
    if portfolio_risk.get("state") == "block":
        add_alert("warning", "PORTFOLIO_RISK_BLOCK", "组合级风控阻断交易", {
            "reason_codes": portfolio_risk.get("reason_codes", []),
            "reasons": portfolio_risk.get("reasons", []),
        })
    elif portfolio_risk.get("state") == "warning":
        add_alert("info", "PORTFOLIO_RISK_WARNING", "组合级风控预警", {
            "reason_codes": portfolio_risk.get("reason_codes", []),
            "reasons": portfolio_risk.get("reasons", []),
        })

    market_signal = str(today_decision.get("market_signal", "")).strip().upper()
    if market_signal in {"RED", "CLEAR"}:
        add_alert("info", f"MARKET_{market_signal}", "当前市场状态不支持主动开仓", {
            "market_signal": market_signal,
        })

    advisory_summary = shadow_snapshot.get("advisory_summary", {}) or {}
    if int(advisory_summary.get("triggered_signal_count", 0) or 0) > 0:
        add_alert("info", "SHADOW_ADVISORY", "影子盘存在 advisory 风控提示", {
            "triggered_rules": advisory_summary.get("triggered_rules", []),
            "triggered_position_count": advisory_summary.get("triggered_position_count", 0),
        })

    levels = [item["level"] for item in alerts]
    overall = "ok"
    if "warning" in levels:
        overall = "warning"
    elif "info" in levels:
        overall = "info"

    return {
        "status": overall,
        "alert_count": len(alerts),
        "alerts": alerts,
        "signal_bus_state": signal_bus.get("state", ""),
        "pool_snapshot_date": (pool_snapshot or {}).get("snapshot_date", ""),
    }


def _combined_state_audit() -> dict:
    base_audit = audit_state()
    shadow_snapshot = _shadow_trade_snapshot()
    try:
        paper_portfolio = load_portfolio_snapshot(scope="paper_mx")
        paper_summary = paper_portfolio.get("summary", {}) if isinstance(paper_portfolio, dict) else {}
        paper_portfolio_check = {
            "ok": True,
            "status": "ok",
            "scope": "paper_mx",
            "as_of_date": paper_portfolio.get("as_of_date", "") if isinstance(paper_portfolio, dict) else "",
            "holding_count": int(paper_summary.get("holding_count", 0) or 0),
            "cash_value": float(paper_summary.get("cash_value", 0.0) or 0.0),
            "total_capital": float(paper_summary.get("total_capital", 0.0) or 0.0),
            "current_exposure": float(paper_summary.get("current_exposure", 0.0) or 0.0),
            "source": "load_portfolio_snapshot",
        }
    except Exception as exc:
        paper_portfolio_check = {
            "ok": False,
            "status": "error",
            "scope": "paper_mx",
            "error": str(exc),
            "source": "load_portfolio_snapshot",
        }
    checks = dict(base_audit.get("checks", {}))
    checks["paper_trade_consistency"] = shadow_snapshot.get("consistency", {})
    checks["paper_portfolio_snapshot"] = paper_portfolio_check

    pool_ok = base_audit.get("status") == "ok"
    paper_check = shadow_snapshot.get("consistency", {})
    paper_ok = bool(paper_check.get("ok"))
    paper_portfolio_ok = bool(paper_portfolio_check.get("ok"))
    if pool_ok and paper_ok and paper_portfolio_ok:
        status = "ok"
    elif pool_ok and (paper_check.get("status") == "error" or paper_portfolio_check.get("status") == "error"):
        status = "warning"
    else:
        status = "drift"

    return {
        "status": status,
        "snapshot_date": base_audit.get("snapshot_date", ""),
        "checks": checks,
    }


def doctor() -> dict:
    started_at = now_ts()
    vault = ObsidianVault()
    checks = {}

    checks["python"] = {
        "ok": bool(sys.executable),
        "executable": sys.executable,
        "version": platform.python_version(),
    }

    mx_key = os.environ.get("MX_APIKEY", "")
    if not mx_key:
        env_path = PROJECT_ROOT / ".env"
        if env_path.exists():
            for line in env_path.read_text(encoding="utf-8").splitlines():
                if line.startswith("MX_APIKEY="):
                    mx_key = line.split("=", 1)[1].strip()
                    break
    checks["mx_apikey"] = {"ok": bool(mx_key), "configured": bool(mx_key)}

    webhook = os.environ.get("DISCORD_WEBHOOK_URL") or get_notification().get("discord", {}).get("webhook_url", "")
    checks["discord_webhook"] = {"ok": bool(webhook), "configured": bool(webhook)}

    vault_path = Path(vault.vault_path)
    checks["vault"] = {
        "ok": vault_path.exists() and vault_path.is_dir(),
        "path": str(vault_path),
        "exists": vault_path.exists(),
    }

    checks["writable"] = {
        "cache": _check_path_writable(CACHE_DIR),
        "runtime": _check_path_writable(RUNTIME_DIR),
        "runs": _check_path_writable(RUNS_DIR),
        "locks": _check_path_writable(LOCK_DIR),
        "ledger": _check_path_writable(Path(LEDGER_DB_PATH).parent),
        "screening": _check_path_writable(vault_path / "04-选股" / "筛选结果"),
    }

    try:
        latest_state = load_daily_state()
        pipeline_names = sorted({
            PIPELINE_ALIASES.get(name, name)
            for name in latest_state.get("pipelines", {}).keys()
        })
        checks["daily_state"] = {
            "ok": True,
            "date": latest_state.get("date"),
            "pipelines": pipeline_names,
        }
    except Exception as e:
        checks["daily_state"] = {"ok": False, "error": str(e)}

    try:
        state_audit = _combined_state_audit()
        checks["state_audit"] = {
            "ok": state_audit.get("status") == "ok",
            "status": state_audit.get("status", "drift"),
            "snapshot_date": state_audit.get("snapshot_date", ""),
            "checks": state_audit.get("checks", {}),
        }
    except Exception as e:
        checks["state_audit"] = {"ok": False, "error": str(e)}

    try:
        checks["data_source_health"] = _data_source_health_snapshot()
    except Exception as e:
        checks["data_source_health"] = {
            "ok": False,
            "status": "warning",
            "warning": ["data_source_health_check_failed"],
            "error": str(e),
        }

    checks["mx_connectivity"] = _requests_ok("https://mkapi2.dfcfs.com/")
    checks["akshare_connectivity"] = _requests_ok("https://push2.eastmoney.com/")

    hard_fail = []
    warning = []

    if not checks["python"]["ok"]:
        hard_fail.append("python")
    if not checks["vault"]["ok"]:
        hard_fail.append("vault")
    for key, item in checks["writable"].items():
        if not item["ok"]:
            hard_fail.append(f"writable:{key}")
    if not checks["mx_apikey"]["ok"]:
        warning.append("mx_apikey")
    if not checks["discord_webhook"]["ok"]:
        warning.append("discord_webhook")
    if not checks["mx_connectivity"]["ok"]:
        warning.append("mx_connectivity")
    if not checks["akshare_connectivity"]["ok"]:
        warning.append("akshare_connectivity")
    if not checks["state_audit"]["ok"]:
        warning.append("state_audit")
    if checks["data_source_health"].get("status") in {"warning", "error"}:
        warning.append("data_source_health")

    status = "success"
    if hard_fail:
        status = "error"
    elif warning:
        status = "warning"

    return {
        "command": "doctor",
        "status": status,
        "retryable": False,
        "started_at": started_at,
        "finished_at": now_ts(),
        "hard_fail": hard_fail,
        "warning": warning,
        "checks": checks,
    }


def _preflight_state_sync(target: str = "all") -> dict:
    result = {"status": "success", "target": target, "steps": []}
    if target in {"portfolio", "all"}:
        result["steps"].append({"step": "portfolio", **sync_portfolio_state()})
    if target in {"activity", "all"}:
        result["steps"].append({"step": "activity", **sync_activity_state()})
    return sanitize_for_json(result)


def state_command(action: str, args) -> dict:
    if action == "bootstrap":
        result = bootstrap_state(force=getattr(args, "force", False))
    elif action == "sync":
        target = getattr(args, "target", "all")
        result = {"status": "success", "target": target, "steps": []}
        if target in {"portfolio", "all"}:
            portfolio_result = sync_portfolio_state()
            result["steps"].append({"step": "portfolio", **portfolio_result})
        if target in {"activity", "all"}:
            activity_result = sync_activity_state()
            result["steps"].append({"step": "activity", **activity_result})
    elif action == "reconcile":
        from scripts.pipeline.shadow_trade import reconcile_trade_state

        result = reconcile_trade_state(
            apply=getattr(args, "apply", False),
            window=getattr(args, "window", 180),
        )
    elif action == "orders":
        snapshot = _compact_order_snapshot(
            load_order_snapshot(
                scope=getattr(args, "scope", None),
                status=getattr(args, "status", None),
            )
        )
        result = {
            **snapshot,
            "status": "ok",
            "scope_filter": snapshot.get("scope", "all"),
            "order_status_filter": snapshot.get("status", "all"),
        }
    elif action == "confirm":
        result = apply_order_reply(
            reply_text=getattr(args, "reply", ""),
            scope=getattr(args, "scope", "paper_mx"),
        )
        result["scope_filter"] = getattr(args, "scope", "paper_mx")
    elif action == "remind":
        scope = getattr(args, "scope", "paper_mx")
        pending = pending_condition_order_items(scope=scope)
        content = render_condition_order_reminder(pending)
        send = bool(getattr(args, "send", False))
        discord_ok = False
        discord_error = ""
        if send:
            discord_ok, discord_error = send_condition_order_reminder(pending)
        result = {
            "status": "ok" if (not send or discord_ok) else "warning",
            "scope_filter": scope,
            "pending_count": len(pending),
            "pending": pending,
            "send": send,
            "discord_ok": discord_ok,
            "discord_error": discord_error,
            "content": content,
        }
    elif action == "pool-actions":
        result = load_pool_action_history(
            limit=getattr(args, "limit", 50),
            snapshot_date=getattr(args, "snapshot_date", None),
        )
        result["status"] = "ok"
    elif action == "trade-review":
        result = load_trade_review(
            window=getattr(args, "window", 90),
            scope=getattr(args, "scope", "cn_a_system"),
        )
        result["status"] = "ok"
    elif action == "alerts":
        strategy = get_strategy()
        today_decision = build_today_decision(strategy=strategy)
        pool_snapshot = load_pool_snapshot()
        pool_sync_state = audit_state()
        market_snapshot = load_market_snapshot()
        shadow_snapshot = _shadow_trade_snapshot()
        order_snapshot = _compact_order_snapshot(load_order_snapshot(scope="paper_mx"))
        signal_bus = build_signal_bus_summary(
            market_snapshot=market_snapshot,
            pool_snapshot=pool_snapshot,
            pool_audit=pool_sync_state,
            today_decision=today_decision,
            shadow_snapshot=shadow_snapshot,
        )
        result = load_alert_snapshot(context={
            "today_decision": today_decision,
            "pool_sync_state": pool_sync_state,
            "shadow_snapshot": shadow_snapshot,
            "order_snapshot": order_snapshot,
            "signal_bus": signal_bus,
            "pool_snapshot": pool_snapshot,
            "market_snapshot": market_snapshot,
        })
    else:
        result = _combined_state_audit()
    return sanitize_for_json({
        "command": "state",
        "action": action,
        "db_path": str(LEDGER_DB_PATH),
        **result,
    })


def mx_command(action: str, args) -> dict:
    if action == "list":
        include_unavailable = bool(getattr(args, "include_unavailable", False))
        items = list_mx_command_metadata(include_unavailable=include_unavailable)
        return sanitize_for_json({
            "command": "mx",
            "action": "list",
            "status": "ok",
            "include_unavailable": include_unavailable,
            "item_count": len(items),
            "items": items,
        })

    if action == "groups":
        include_unavailable = bool(getattr(args, "include_unavailable", False))
        groups = mx_command_groups(include_unavailable=include_unavailable)
        return sanitize_for_json({
            "command": "mx",
            "action": "groups",
            "status": "ok",
            "include_unavailable": include_unavailable,
            "group_count": len(groups),
            "groups": groups,
        })

    if action == "health":
        include_unavailable = bool(getattr(args, "include_unavailable", False))
        health = _mx_health_snapshot(include_unavailable=include_unavailable)
        return sanitize_for_json({
            "command": "mx",
            "action": "health",
            "status": health["status"],
            "include_unavailable": include_unavailable,
            "health": health,
        })

    # 构建 kwargs，只包含当前命令 spec 定义的参数
    from scripts.mx.cli_tools import build_mx_command_registry, get_mx_command_spec
    cmd_name = getattr(args, "mx_command")
    spec = get_mx_command_spec(cmd_name, include_unavailable=True)
    spec_arg_names = {arg.name for arg in spec.args}

    kwargs = {}
    for field in ("query", "stock_code", "quantity", "price", "use_market_price", "order_id", "cancel_all"):
        if field not in spec_arg_names:
            continue
        value = getattr(args, field, None)
        if value is not None:
            kwargs[field] = value
    try:
        result = dispatch_mx_command(cmd_name, **kwargs)
        return sanitize_for_json({
            "command": "mx",
            "action": "run",
            "status": "ok",
            "mx_command": getattr(args, "mx_command"),
            "arguments": kwargs,
            "result": result,
        })
    except MXCommandError as exc:
        return sanitize_for_json({
            "command": "mx",
            "action": "run",
            "status": "error",
            "mx_command": getattr(args, "mx_command"),
            "arguments": kwargs,
            "error": str(exc),
        })


def run_pipeline(name: str, args) -> dict:
    started = datetime.now()
    run_id = make_run_id(name)
    payload = {
        "command": "run",
        "pipeline": name,
        "run_id": run_id,
        "started_at": started.strftime("%Y-%m-%dT%H:%M:%S"),
        "retryable": False,
        "status": "error",
        "details": {},
    }

    try:
        state_sync = _preflight_state_sync("all")
        payload["state_sync"] = state_sync
        if state_sync.get("status") == "error":
            payload["status"] = "blocked"
            payload["error"] = "state_sync_failed"
            payload["details"] = {"state_sync": state_sync}
            payload["next_actions"] = _recommend_next_actions(
                status=payload["status"],
                workflow_name=None,
                error=payload["error"],
                retryable=payload["retryable"],
            )
            return _finalize_run(payload, started)

        doctor_result = doctor()
        payload["doctor"] = {
            "status": doctor_result["status"],
            "hard_fail": doctor_result.get("hard_fail", []),
            "warning": doctor_result.get("warning", []),
        }
        if doctor_result["status"] == "error":
            payload["status"] = "blocked"
            payload["error"] = "doctor_failed"
            payload["details"] = {"doctor": doctor_result}
            payload["next_actions"] = _recommend_next_actions(
                status=payload["status"],
                workflow_name=None,
                error=payload["error"],
                retryable=payload["retryable"],
            )
            return _finalize_run(payload, started)

        with pipeline_lock(name):
            result = PIPELINES[name](args)
            payload["result"] = _normalize_pipeline_result(name, result)
            payload["details"] = _summarize_pipeline_result(name, result, args)
            payload["status"] = "success"
            if doctor_result["status"] == "warning":
                payload["status"] = "warning"
    except RuntimeError as e:
        payload["status"] = "blocked"
        payload["retryable"] = True
        payload["error"] = "pipeline_locked"
        try:
            payload["details"] = {"lock": json.loads(str(e))}
        except Exception:
            payload["details"] = {"lock": str(e)}
    except Exception as e:
        payload["status"] = "error"
        payload["retryable"] = True
        payload["error"] = str(e)
        payload["traceback"] = traceback.format_exc()

    payload["next_actions"] = _recommend_next_actions(
        status=payload["status"],
        workflow_name=None,
        error=payload.get("error", ""),
        retryable=payload.get("retryable", False),
    )
    return _finalize_run(payload, started)


def _summarize_pipeline_result(name: str, result, args) -> dict:
    summary = {"result_type": type(result).__name__}
    if name == "screener":
        rows = result or []
        summary.update({
            "pool": args.pool,
            "universe": args.universe,
            "count": len(rows),
            "top_codes": [row.get("code", "") for row in rows[:5]],
        })
    elif isinstance(result, dict):
        for key in ["review_path", "tomorrow_date", "weekly_bought"]:
            if key in result:
                summary[key] = result.get(key)
        market_data = result.get("market_data") or result.get("market_snapshot")
        if isinstance(market_data, dict):
            summary["market_signal"] = market_data.get("signal", market_data.get("market_signal", ""))
    elif isinstance(result, list):
        summary["count"] = len(result)
    return summary


def _normalize_pipeline_result(name: str, result):
    if name == "scoring" and isinstance(result, list):
        normalized = []
        for row in result:
            normalized.append({
                "name": row.get("name", ""),
                "code": row.get("code", ""),
                "technical_score": row.get("technical_score", 0),
                "fundamental_score": row.get("fundamental_score", 0),
                "flow_score": row.get("flow_score", 0),
                "sentiment_score": row.get("sentiment_score", 0),
                "total_score": row.get("total_score", 0),
                "veto_signals": row.get("veto_signals", []),
                "veto_triggered": row.get("veto_triggered", False),
                "technical_detail": row.get("technical_detail", ""),
                "fundamental_detail": row.get("fundamental_detail", ""),
                "flow_detail": row.get("flow_detail", ""),
                "sentiment_detail": row.get("sentiment_detail", ""),
                "data_quality": row.get("data_quality", "ok"),
                "data_missing_fields": row.get("data_missing_fields", []),
            })
        return normalized
    if name == "screener" and isinstance(result, list):
        normalized = []
        for row in result:
            normalized.append({
                "name": row.get("name", ""),
                "code": row.get("code", ""),
                "total_score": row.get("total_score", 0),
                "technical_score": row.get("technical_score", 0),
                "fundamental_score": row.get("fundamental_score", 0),
                "flow_score": row.get("flow_score", 0),
                "sentiment_score": row.get("sentiment_score", 0),
                "veto_triggered": row.get("veto_triggered", False),
                "veto_signals": row.get("veto_signals", []),
                "data_quality": row.get("data_quality", "ok"),
                "data_missing_fields": row.get("data_missing_fields", []),
            })
        return normalized
    if name in {"morning", "noon", "evening", "weekly"} and isinstance(result, dict):
        keep = {}
        for key in ["market_data", "positions", "core_pool", "weekly_bought", "review_path", "summary", "tomorrow_date"]:
            if key in result:
                keep[key] = result[key]
        if not keep:
            keep = result
        return keep
    return result


def _finalize_run(payload: dict, started: datetime) -> dict:
    finished = datetime.now()
    payload["finished_at"] = finished.strftime("%Y-%m-%dT%H:%M:%S")
    payload["duration_seconds"] = round((finished - started).total_seconds(), 3)
    payload["result_path"] = write_run_result(payload)
    payload["daily_state_path"] = sync_run_to_daily_state(payload)
    return sanitize_for_json(payload)


def _artifact_paths_from_run(run_result: dict) -> list:
    artifacts = []
    details = run_result.get("details", {}) if isinstance(run_result, dict) else {}
    result = run_result.get("result", {}) if isinstance(run_result, dict) else {}

    for key in [
        "report_path",
        "review_path",
        "market_watch_path",
        "suggestion_path",
        "pool_state_path",
        "daily_state_path",
        "result_path",
    ]:
        value = details.get(key)
        if value:
            artifacts.append({"type": key, "path": value})

    if isinstance(result, dict):
        for key in ["review_path", "tomorrow_date"]:
            value = result.get(key)
            if value:
                artifacts.append({"type": key, "path": value})

    if run_result.get("result_path"):
        artifacts.append({"type": "run_result", "path": run_result["result_path"]})
    if run_result.get("daily_state_path"):
        artifacts.append({"type": "daily_state", "path": run_result["daily_state_path"]})

    deduped = []
    seen = set()
    for item in artifacts:
        path = item.get("path", "")
        if not path or path in seen:
            continue
        seen.add(path)
        deduped.append(item)
    return deduped


def status_today(sync_state: bool = True) -> dict:
    if sync_state:
        _preflight_state_sync("all")
    today = load_daily_state()
    strategy = get_strategy()
    today_decision = build_today_decision(strategy=strategy)
    portfolio_snapshot = load_portfolio_snapshot(scope="cn_a_system")
    paper_portfolio_snapshot = load_portfolio_snapshot(scope="paper_mx")
    pool_snapshot = load_pool_snapshot()
    pool_sync_state = audit_state()
    market_snapshot = load_market_snapshot()
    shadow_snapshot = _shadow_trade_snapshot()
    mx_health = _mx_health_snapshot(include_unavailable=True)
    try:
        order_snapshot = _compact_order_snapshot(load_order_snapshot(scope="paper_mx"))
    except Exception as e:
        order_snapshot = {
            "scope": "paper_mx",
            "status": "error",
            "db_path": str(LEDGER_DB_PATH),
            "summary": {
                "order_count": 0,
                "pending_count": 0,
                "open_count": 0,
                "exception_count": 0,
                "status_counts": {},
                "scope_counts": {},
                "class_counts": {},
            },
            "condition_orders": {
                "count": 0,
                "pending_count": 0,
                "open_count": 0,
                "exception_count": 0,
                "status_counts": {},
                "condition_type_counts": {},
                "sample": [],
                "error": str(e),
            },
        }
    signal_bus = build_signal_bus_summary(
        market_snapshot=market_snapshot,
        pool_snapshot=pool_snapshot,
        pool_audit=pool_sync_state,
        today_decision=today_decision,
        shadow_snapshot=shadow_snapshot,
    )
    alert_snapshot = load_alert_snapshot(context={
        "today_decision": today_decision,
        "pool_sync_state": pool_sync_state,
        "shadow_snapshot": shadow_snapshot,
        "order_snapshot": order_snapshot,
        "signal_bus": signal_bus,
        "pool_snapshot": pool_snapshot,
        "market_snapshot": market_snapshot,
    })
    pipelines = today.get("pipelines", {})
    normalized = {}
    for name, payload in pipelines.items():
        canonical = PIPELINE_ALIASES.get(name, name)
        if canonical not in normalized:
            normalized[canonical] = payload
            continue
        existing_updated = normalized[canonical].get("updated_at", "")
        current_updated = payload.get("updated_at", "")
        if current_updated >= existing_updated:
            normalized[canonical] = payload
    return {
        "command": "status",
        "date": today.get("date"),
        "pipelines": normalized,
        "updated_at": today.get("updated_at", ""),
        "today_decision": today_decision,
        "positions_summary": portfolio_snapshot.get("summary", {}),
        "paper_mx_portfolio": {
            "scope": paper_portfolio_snapshot.get("scope", "paper_mx"),
            "as_of_date": paper_portfolio_snapshot.get("as_of_date", ""),
            "summary": paper_portfolio_snapshot.get("summary", {}),
        },
        "market_snapshot": market_snapshot,
        "market_signal": market_snapshot.get("signal", market_snapshot.get("market_signal", "")),
        "market_snapshot_source": {
            "source": market_snapshot.get("source", ""),
            "source_chain": market_snapshot.get("source_chain", []),
            "as_of_date": market_snapshot.get("as_of_date", ""),
        },
        "mx_health": mx_health,
        "signal_bus": signal_bus,
        "alert_snapshot": alert_snapshot,
        "pool_sync_state": pool_sync_state,
        "paper_trade_audit": shadow_snapshot.get("consistency", {}),
        "order_snapshot": order_snapshot,
        "shadow_trade_state": {
            "status": shadow_snapshot.get("status", "error"),
            "timestamp": shadow_snapshot.get("timestamp", ""),
            "positions_count": shadow_snapshot.get("positions_count", 0),
            "automation_scope": shadow_snapshot.get("automation_scope", ""),
            "advisory_summary": shadow_snapshot.get("advisory_summary", {}),
            "mx_health": shadow_snapshot.get("mx_health", {}),
        },
        "rule_automation_scope": AUTOMATED_RULES,
        "pool_management": {
            "updated_at": pool_snapshot.get("updated_at", ""),
            "last_eval_date": pool_snapshot.get("snapshot_date", ""),
            "summary": pool_snapshot.get("summary", {}),
            "action_history_summary": pool_snapshot.get("action_history_summary", {}),
            "state_path": str(LEDGER_DB_PATH),
        },
    }


def orchestrate_workflow(name: str, args) -> dict:
    started = datetime.now()
    workflow = WORKFLOWS[name]
    workflow_steps = workflow["steps"]
    payload = {
        "command": "orchestrate",
        "workflow": name,
        "workflow_spec": workflow,
        "status": "success",
        "started_at": started.strftime("%Y-%m-%dT%H:%M:%S"),
        "retryable": False,
        "steps": [],
        "artifacts": [],
    }

    try:
        state_sync = _preflight_state_sync("all")
        payload["state_sync"] = state_sync
        payload["steps"].append({"step": "state_sync", "status": state_sync.get("status", "success")})
        if state_sync.get("status") == "error":
            payload["status"] = "blocked"
            payload["error"] = "state_sync_failed"
            payload["next_actions"] = _recommend_next_actions(
                status=payload["status"],
                workflow_name=name,
                error=payload["error"],
                retryable=payload["retryable"],
            )
            finished = datetime.now()
            payload["finished_at"] = finished.strftime("%Y-%m-%dT%H:%M:%S")
            payload["duration_seconds"] = round((finished - started).total_seconds(), 3)
            return sanitize_for_json(payload)
    except Exception as e:
        payload["status"] = "blocked"
        payload["error"] = "state_sync_failed"
        payload["steps"].append({"step": "state_sync", "status": "error", "error": str(e)})
        payload["next_actions"] = _recommend_next_actions(
            status=payload["status"],
            workflow_name=name,
            error=payload["error"],
            retryable=payload["retryable"],
        )
        finished = datetime.now()
        payload["finished_at"] = finished.strftime("%Y-%m-%dT%H:%M:%S")
        payload["duration_seconds"] = round((finished - started).total_seconds(), 3)
        return sanitize_for_json(payload)

    doctor_result = doctor()
    payload["doctor"] = {
        "status": doctor_result.get("status", "error"),
        "hard_fail": doctor_result.get("hard_fail", []),
        "warning": doctor_result.get("warning", []),
    }
    if doctor_result.get("status") == "error":
        payload["status"] = "blocked"
        payload["error"] = "doctor_failed"
        payload["steps"].append({"step": "doctor", "status": "error"})
        payload["next_actions"] = _recommend_next_actions(
            status=payload["status"],
            workflow_name=name,
            error=payload["error"],
            retryable=payload["retryable"],
        )
        finished = datetime.now()
        payload["finished_at"] = finished.strftime("%Y-%m-%dT%H:%M:%S")
        payload["duration_seconds"] = round((finished - started).total_seconds(), 3)
        return sanitize_for_json(payload)

    status_before = status_today(sync_state=False)
    payload["status_before"] = {
        "today_decision": status_before.get("today_decision", {}),
        "pool_management": status_before.get("pool_management", {}),
    }
    payload["steps"].append({"step": "status_before", "status": "success"})

    run_targets = workflow_steps[1:]
    for target in run_targets:
        if target == "screener":
            default_args = workflow.get("default_args", {})
            run_args = argparse.Namespace(
                command="run",
                pipeline="screener",
                pool=getattr(args, "pool", default_args.get("pool", "all")),
                universe=getattr(args, "universe", default_args.get("universe", "tracked")),
                json=getattr(args, "json", False),
            )
        else:
            run_args = argparse.Namespace(
                command="run",
                pipeline=target,
                pool="all",
                universe="tracked",
                json=getattr(args, "json", False),
            )

        step_result = run_pipeline(target, run_args)
        payload["steps"].append({
            "step": target,
            "status": step_result.get("status", "error"),
            "run_id": step_result.get("run_id", ""),
            "result_path": step_result.get("result_path", ""),
        })
        payload["artifacts"].extend(_artifact_paths_from_run(step_result))

        if step_result.get("status") in {"error", "blocked"}:
            payload["status"] = step_result.get("status", "error")
            payload["error"] = step_result.get("error", f"{target}_failed")
            payload["failed_step"] = target
            break
        if step_result.get("status") == "warning" and payload["status"] == "success":
            payload["status"] = "warning"

    status_after = status_today(sync_state=False)
    payload["status_after"] = {
        "today_decision": status_after.get("today_decision", {}),
        "pool_management": status_after.get("pool_management", {}),
        "pipelines": status_after.get("pipelines", {}),
    }
    payload["steps"].append({"step": "status_after", "status": "success"})
    payload["next_actions"] = _recommend_next_actions(
        status=payload["status"],
        workflow_name=name,
        error=payload.get("error", ""),
        retryable=payload.get("retryable", False),
    )

    finished = datetime.now()
    payload["finished_at"] = finished.strftime("%Y-%m-%dT%H:%M:%S")
    payload["duration_seconds"] = round((finished - started).total_seconds(), 3)
    return sanitize_for_json(payload)


def main():
    argv = [arg for arg in sys.argv[1:] if arg != "--json"]
    json_output = any(arg == "--json" for arg in sys.argv[1:])

    parser = argparse.ArgumentParser(description="Trade system unified CLI")
    parser.add_argument("--json", action="store_true", help="Output JSON")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("doctor", help="Run health checks")
    sub.add_parser("workflows", help="List shared workflows for Hermes/OpenClaw")
    sub.add_parser("templates", help="List agent response templates")

    mx_parser = sub.add_parser("mx", help="Run MX capability wrappers")
    mx_sub = mx_parser.add_subparsers(dest="action", required=True)
    mx_list = mx_sub.add_parser("list")
    mx_list.add_argument("--include-unavailable", action="store_true", help="Include unavailable MX capabilities")
    mx_groups = mx_sub.add_parser("groups")
    mx_groups.add_argument("--include-unavailable", action="store_true", help="Include unavailable MX capabilities")
    mx_health = mx_sub.add_parser("health")
    mx_health.add_argument("--include-unavailable", action="store_true", help="Include unavailable MX capabilities")
    mx_run = mx_sub.add_parser("run")
    mx_run.add_argument("mx_command", help="MX command id or alias")
    mx_run.add_argument("--query", default=None, help="Natural language query for data/search/xuangu/zixuan manage")
    mx_run.add_argument("--stock-code", default=None, help="6-digit stock code for moni trade commands")
    mx_run.add_argument("--quantity", type=int, default=None, help="Share quantity for moni trade commands")
    mx_run.add_argument("--price", type=float, default=None, help="Limit price for moni trade commands")
    mx_run.add_argument("--use-market-price", action="store_true", help="Use market price for moni trade commands")
    mx_run.add_argument("--order-id", default=None, help="Order id for moni cancel command")
    mx_run.add_argument("--cancel-all", action="store_true", help="Cancel all moni orders")

    state_parser = sub.add_parser("state", help="Manage structured ledger state")
    state_sub = state_parser.add_subparsers(dest="action", required=True)
    state_bootstrap = state_sub.add_parser("bootstrap")
    state_bootstrap.add_argument("--force", action="store_true", help="Rebuild ledger from current markdown/config")
    state_sync = state_sub.add_parser("sync")
    state_sync.add_argument("--target", choices=["portfolio", "activity", "all"], default="all")
    state_sub.add_parser("audit")
    state_reconcile = state_sub.add_parser("reconcile")
    state_reconcile.add_argument("--apply", action="store_true", help="Write reconcile events into paper ledger/log")
    state_reconcile.add_argument("--window", type=int, default=180, help="Lookback window for paper event inference")
    state_orders = state_sub.add_parser("orders")
    state_orders.add_argument("--scope", default=None, help="Optional scope filter for structured orders")
    state_orders.add_argument("--status", default=None, help="Optional status filter for structured orders")
    state_confirm = state_sub.add_parser("confirm")
    state_confirm.add_argument("--reply", required=True, help="Discord/manual reply text for condition-order confirmation")
    state_confirm.add_argument("--scope", default="paper_mx", help="Scope for structured order confirmation")
    state_remind = state_sub.add_parser("remind")
    state_remind.add_argument("--scope", default="paper_mx", help="Scope for pending condition-order reminders")
    state_remind.add_argument("--send", action="store_true", help="Send reminder to Discord webhook")
    state_pool_actions = state_sub.add_parser("pool-actions")
    state_pool_actions.add_argument("--limit", type=int, default=50, help="Maximum number of pool actions to return")
    state_pool_actions.add_argument("--snapshot-date", default=None, help="Optional YYYY-MM-DD snapshot date filter")
    state_trade_review = state_sub.add_parser("trade-review")
    state_trade_review.add_argument("--window", type=int, default=90, help="Lookback window for structured trade review")
    state_trade_review.add_argument("--scope", default="cn_a_system", help="Scope for structured trade review")
    state_sub.add_parser("alerts")

    backtest_parser = sub.add_parser("backtest", help="Run structured backtest and walk-forward summaries")
    backtest_sub = backtest_parser.add_subparsers(dest="action", required=True)
    backtest_run = backtest_sub.add_parser("run")
    backtest_run.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    backtest_run.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    backtest_run.add_argument("--scope", default="cn_a_system", help="Scope for backtest")
    backtest_run.add_argument("--fixture", default=None, help="Optional JSON fixture path for backtest inputs")
    backtest_run.add_argument("--buy-thresholds", default=None, help="Comma-separated buy thresholds")
    backtest_run.add_argument("--stop-losses", default=None, help="Comma-separated stop loss values")
    backtest_run.add_argument("--take-profits", default=None, help="Comma-separated take profit values")
    backtest_run.add_argument("--technical-weights", default=None, help="Comma-separated technical weight values")
    backtest_run.add_argument("--fundamental-weights", default=None, help="Comma-separated fundamental weight values")
    backtest_run.add_argument("--flow-weights", default=None, help="Comma-separated flow weight values")
    backtest_run.add_argument("--sentiment-weights", default=None, help="Comma-separated sentiment weight values")
    backtest_sweep = backtest_sub.add_parser("sweep")
    backtest_sweep.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    backtest_sweep.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    backtest_sweep.add_argument("--scope", default="cn_a_system", help="Scope for sweep")
    backtest_sweep.add_argument("--fixture", default=None, help="Optional JSON fixture path for backtest inputs")
    backtest_sweep.add_argument("--buy-thresholds", default=None, help="Comma-separated buy thresholds")
    backtest_sweep.add_argument("--stop-losses", default=None, help="Comma-separated stop loss values")
    backtest_sweep.add_argument("--take-profits", default=None, help="Comma-separated take profit values")
    backtest_sweep.add_argument("--technical-weights", default=None, help="Comma-separated technical weight values")
    backtest_sweep.add_argument("--fundamental-weights", default=None, help="Comma-separated fundamental weight values")
    backtest_sweep.add_argument("--flow-weights", default=None, help="Comma-separated flow weight values")
    backtest_sweep.add_argument("--sentiment-weights", default=None, help="Comma-separated sentiment weight values")
    backtest_walk = backtest_sub.add_parser("walk-forward")
    backtest_walk.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    backtest_walk.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    backtest_walk.add_argument("--scope", default="cn_a_system", help="Scope for walk-forward")
    backtest_walk.add_argument("--folds", type=int, default=3, help="Walk-forward folds")
    backtest_walk.add_argument("--fixture", default=None, help="Optional JSON fixture path for backtest inputs")
    backtest_walk.add_argument("--buy-thresholds", default=None, help="Comma-separated buy thresholds")
    backtest_walk.add_argument("--stop-losses", default=None, help="Comma-separated stop loss values")
    backtest_walk.add_argument("--take-profits", default=None, help="Comma-separated take profit values")
    backtest_walk.add_argument("--technical-weights", default=None, help="Comma-separated technical weight values")
    backtest_walk.add_argument("--fundamental-weights", default=None, help="Comma-separated fundamental weight values")
    backtest_walk.add_argument("--flow-weights", default=None, help="Comma-separated flow weight values")
    backtest_walk.add_argument("--sentiment-weights", default=None, help="Comma-separated sentiment weight values")
    backtest_history = backtest_sub.add_parser("history")
    backtest_history.add_argument("--limit", type=int, default=10, help="Number of historical backtest entries")
    backtest_compare = backtest_sub.add_parser("compare")
    backtest_compare.add_argument("--limit", type=int, default=20, help="Number of historical backtest entries to compare")
    backtest_replay = backtest_sub.add_parser("strategy-replay")
    backtest_replay.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    backtest_replay.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    backtest_replay.add_argument("--fixture", required=True, help="JSON fixture with daily_data for strategy replay")

    # --- order subcommand (for Hermes-Agent) ---
    order_parser = sub.add_parser("order", help="Order management for Hermes-Agent")
    order_sub = order_parser.add_subparsers(dest="action", required=True)

    order_confirm = order_sub.add_parser("confirm", help="Confirm condition order via parsed reply text")
    order_confirm.add_argument("reply", help="User reply text, e.g. '止损触发了 艾比森 成交¥19.00'")
    order_confirm.add_argument("--scope", default="paper_mx", help="Order scope")

    order_pending = order_sub.add_parser("pending", help="List pending condition orders")
    order_pending.add_argument("--scope", default="paper_mx", help="Order scope")

    order_remind = order_sub.add_parser("remind", help="Send pending order reminders to Discord")
    order_remind.add_argument("--scope", default="paper_mx", help="Order scope")
    order_remind.add_argument("--send", action="store_true", help="Actually send to Discord")

    order_overdue = order_sub.add_parser("overdue-check", help="Check overdue unconfirmed orders and send reminders")
    order_overdue.add_argument("--scope", default="paper_mx", help="Order scope")
    order_overdue.add_argument("--send", action="store_true", help="Actually send reminders to Discord")

    order_place = order_sub.add_parser("place", help="Place a new condition order")
    order_place.add_argument("--code", required=True, help="Stock code")
    order_place.add_argument("--name", required=True, help="Stock name")
    order_place.add_argument("--side", choices=["buy", "sell"], default="sell", help="Order side")
    order_place.add_argument("--type", dest="condition_type", required=True,
                             choices=["dynamic_stop", "absolute_stop", "take_profit_t1", "manual_stop", "manual_tp"],
                             help="Condition type")
    order_place.add_argument("--price", type=float, required=True, help="Trigger price")
    order_place.add_argument("--shares", type=int, default=0, help="Share quantity (0 = all)")
    order_place.add_argument("--scope", default="paper_mx", help="Order scope")
    order_place.add_argument("--reason", default="", help="Reason text")

    order_cancel = order_sub.add_parser("cancel", help="Cancel a condition order")
    order_cancel.add_argument("--code", required=True, help="Stock code")
    order_cancel.add_argument("--name", default="", help="Stock name (for matching)")
    order_cancel.add_argument("--type", dest="condition_type", default="",
                              help="Condition type filter (optional)")
    order_cancel.add_argument("--scope", default="paper_mx", help="Order scope")

    order_modify = order_sub.add_parser("modify", help="Modify trigger price of a condition order")
    order_modify.add_argument("--code", required=True, help="Stock code")
    order_modify.add_argument("--name", default="", help="Stock name (for matching)")
    order_modify.add_argument("--price", type=float, required=True, help="New trigger price")
    order_modify.add_argument("--type", dest="condition_type", default="",
                              help="Condition type filter (optional)")
    order_modify.add_argument("--scope", default="paper_mx", help="Order scope")

    order_list = order_sub.add_parser("list", help="List all orders with optional filters")
    order_list.add_argument("--scope", default=None, help="Scope filter")
    order_list.add_argument("--status", default=None, help="Status filter")
    order_list.add_argument("--limit", type=int, default=20, help="Max results")

    run_parser = sub.add_parser("run", help="Run pipeline")
    run_sub = run_parser.add_subparsers(dest="pipeline", required=True)
    for name in ["morning", "noon", "evening", "scoring", "weekly"]:
        run_sub.add_parser(name)
    screener = run_sub.add_parser("screener")
    screener.add_argument("--pool", choices=["core", "watch", "all"], default="watch")
    screener.add_argument("--universe", choices=["tracked", "market"], default="tracked")
    sentiment_parser = run_sub.add_parser("sentiment")
    sentiment_parser.add_argument("--dry-run", action="store_true", help="Scan only, no Discord push")
    hk_parser = run_sub.add_parser("hk_monitor")
    hk_parser.add_argument("--dry-run", action="store_true", help="Check only, no Discord push")
    monthly_parser = run_sub.add_parser("monthly")
    monthly_parser.add_argument("--month", default=None, help="Month in YYYY-MM format (default: current month)")

    status_parser = sub.add_parser("status", help="Show current status")
    status_parser.add_argument("target", choices=["today"])

    orch_parser = sub.add_parser("orchestrate", help="Run shared workflow for Hermes/OpenClaw")
    orch_parser.add_argument(
        "workflow",
        choices=sorted(WORKFLOWS.keys()),
    )
    orch_parser.add_argument("--pool", choices=["core", "watch", "all"], default="all")
    orch_parser.add_argument("--universe", choices=["tracked", "market"], default="tracked")

    args = parser.parse_args(argv)
    args.json = json_output

    if json_output:
        stdout_buf = io.StringIO()
        stderr_buf = io.StringIO()
        previous_disable = logging.root.manager.disable
        logging.disable(logging.CRITICAL)
        set_console_logging(False)
        try:
            with contextlib.redirect_stdout(stdout_buf), contextlib.redirect_stderr(stderr_buf):
                if args.command == "doctor":
                    result = doctor()
                elif args.command == "mx":
                    result = mx_command(args.action, args)
                elif args.command == "workflows":
                    result = list_workflows()
                elif args.command == "templates":
                    result = list_agent_templates()
                elif args.command == "order":
                    result = order_command(args.action, args)
                elif args.command == "run":
                    result = run_pipeline(args.pipeline, args)
                elif args.command == "orchestrate":
                    result = orchestrate_workflow(args.workflow, args)
                elif args.command == "state":
                    result = state_command(args.action, args)
                elif args.command == "backtest":
                    if args.action == "run":
                        result = run_backtest(
                            start=args.start,
                            end=args.end,
                            scope=args.scope,
                            fixture=args.fixture,
                            buy_thresholds=args.buy_thresholds,
                            stop_losses=args.stop_losses,
                            take_profits=args.take_profits,
                            technical_weights=args.technical_weights,
                            fundamental_weights=args.fundamental_weights,
                            flow_weights=args.flow_weights,
                            sentiment_weights=args.sentiment_weights,
                        )
                    elif args.action == "sweep":
                        result = run_parameter_sweep(
                            start=args.start,
                            end=args.end,
                            scope=args.scope,
                            fixture=args.fixture,
                            buy_thresholds=args.buy_thresholds,
                            stop_losses=args.stop_losses,
                            take_profits=args.take_profits,
                            technical_weights=args.technical_weights,
                            fundamental_weights=args.fundamental_weights,
                            flow_weights=args.flow_weights,
                            sentiment_weights=args.sentiment_weights,
                        )
                    elif args.action == "history":
                        result = list_backtest_history(limit=args.limit)
                    elif args.action == "compare":
                        result = compare_backtest_history(limit=args.limit)
                    elif args.action == "strategy-replay":
                        from scripts.backtest.strategy_replay import run_strategy_replay
                        fixture_data = json.loads(Path(args.fixture).read_text(encoding="utf-8"))
                        result = run_strategy_replay(
                            daily_data=fixture_data.get("daily_data", {}),
                            start=args.start,
                            end=args.end,
                            total_capital=fixture_data.get("total_capital", 450286),
                            params=fixture_data.get("params", {}),
                        )
                    else:
                        result = run_walk_forward(
                            start=args.start,
                            end=args.end,
                            scope=args.scope,
                            folds=args.folds,
                            fixture=args.fixture,
                            buy_thresholds=args.buy_thresholds,
                            stop_losses=args.stop_losses,
                            take_profits=args.take_profits,
                            technical_weights=args.technical_weights,
                            fundamental_weights=args.fundamental_weights,
                            flow_weights=args.flow_weights,
                            sentiment_weights=args.sentiment_weights,
                        )
                else:
                    result = status_today()
        finally:
            logging.disable(previous_disable)
            set_console_logging(True)
        logs = []
        raw_out = stdout_buf.getvalue().strip()
        raw_err = stderr_buf.getvalue().strip()
        if raw_out:
            logs.append(raw_out)
        if raw_err:
            logs.append(raw_err)
        if logs:
            result["logs"] = logs
    else:
        if args.command == "doctor":
            result = doctor()
        elif args.command == "mx":
            result = mx_command(args.action, args)
        elif args.command == "workflows":
            result = list_workflows()
        elif args.command == "templates":
            result = list_agent_templates()
        elif args.command == "order":
            result = order_command(args.action, args)
        elif args.command == "run":
            result = run_pipeline(args.pipeline, args)
        elif args.command == "orchestrate":
            result = orchestrate_workflow(args.workflow, args)
        elif args.command == "state":
            result = state_command(args.action, args)
        elif args.command == "backtest":
            if args.action == "run":
                result = run_backtest(
                    start=args.start,
                    end=args.end,
                    scope=args.scope,
                    fixture=args.fixture,
                    buy_thresholds=args.buy_thresholds,
                    stop_losses=args.stop_losses,
                    take_profits=args.take_profits,
                    technical_weights=args.technical_weights,
                    fundamental_weights=args.fundamental_weights,
                    flow_weights=args.flow_weights,
                    sentiment_weights=args.sentiment_weights,
                )
            elif args.action == "sweep":
                result = run_parameter_sweep(
                    start=args.start,
                    end=args.end,
                    scope=args.scope,
                    fixture=args.fixture,
                    buy_thresholds=args.buy_thresholds,
                    stop_losses=args.stop_losses,
                    take_profits=args.take_profits,
                    technical_weights=args.technical_weights,
                    fundamental_weights=args.fundamental_weights,
                    flow_weights=args.flow_weights,
                    sentiment_weights=args.sentiment_weights,
                )
            elif args.action == "history":
                result = list_backtest_history(limit=args.limit)
            elif args.action == "compare":
                result = compare_backtest_history(limit=args.limit)
            elif args.action == "strategy-replay":
                from scripts.backtest.strategy_replay import run_strategy_replay
                fixture_data = json.loads(Path(args.fixture).read_text(encoding="utf-8"))
                result = run_strategy_replay(
                    daily_data=fixture_data.get("daily_data", {}),
                    start=args.start,
                    end=args.end,
                    total_capital=fixture_data.get("total_capital", 450286),
                    params=fixture_data.get("params", {}),
                )
            else:
                result = run_walk_forward(
                    start=args.start,
                    end=args.end,
                    scope=args.scope,
                    folds=args.folds,
                    fixture=args.fixture,
                    buy_thresholds=args.buy_thresholds,
                    stop_losses=args.stop_losses,
                    take_profits=args.take_profits,
                    technical_weights=args.technical_weights,
                    fundamental_weights=args.fundamental_weights,
                    flow_weights=args.flow_weights,
                    sentiment_weights=args.sentiment_weights,
                )
        else:
            result = status_today()

    if args.json:
        _json_print(result)
    else:
        if result.get("command") == "doctor":
            print(f"doctor: {result['status']}")
            if result.get("hard_fail"):
                print("hard_fail:", ", ".join(result["hard_fail"]))
            if result.get("warning"):
                print("warning:", ", ".join(result["warning"]))
        elif result.get("command") == "mx":
            print(f"mx {result.get('action')}: {result.get('status', 'ok')}")
            if result.get("action") == "list":
                print(f"item_count: {result.get('item_count', 0)}")
            elif result.get("action") == "groups":
                print(f"group_count: {result.get('group_count', 0)}")
            elif result.get("action") == "health":
                health = result.get("health", {})
                print(
                    "health: "
                    f"available={health.get('available_count', 0)} "
                    f"unavailable={health.get('unavailable_count', 0)} "
                    f"commands={health.get('command_count', 0)}"
                )
            else:
                print(f"mx_command: {result.get('mx_command', '')}")
        elif result.get("command") == "workflows":
            for item in result.get("items", []):
                print(f"{item['name']}: {', '.join(item.get('steps', []))}")
        elif result.get("command") == "templates":
            for status, agents in result.get("items", {}).items():
                print(f"{status}: {', '.join(sorted(agents.keys()))}")
        elif result.get("command") == "status":
            print(f"status today: {result.get('date')}")
            print(f"today_decision: {result.get('today_decision', {}).get('decision')}")
            print(f"market_signal: {result.get('market_signal', '')}")
            print("pipelines:", ", ".join(sorted(result.get("pipelines", {}).keys())))
        elif result.get("command") == "state":
            print(f"state {result.get('action')}: {result.get('status', 'ok')}")
            print(f"db_path: {result.get('db_path', '')}")
            if result.get("action") == "orders":
                summary = result.get("summary", {})
                print(
                    "orders: "
                    f"total={summary.get('order_count', 0)} "
                    f"pending={summary.get('pending_count', 0)} "
                    f"open={summary.get('open_count', 0)} "
                    f"exception={summary.get('exception_count', 0)}"
                )
            elif result.get("action") == "confirm":
                print(f"reply: {result.get('reply', {}).get('raw', '')}")
                print(f"matched_order_count: {result.get('matched_order_count', 0)}")
                print(f"trade_event_recorded: {result.get('trade_event_recorded', False)}")
            elif result.get("action") == "remind":
                print(f"pending_count: {result.get('pending_count', 0)}")
                print(f"send: {result.get('send', False)} discord_ok={result.get('discord_ok', False)}")
            elif result.get("action") == "pool-actions":
                print(f"pool_actions: {result.get('action_count', 0)}")
            elif result.get("action") == "trade-review":
                print(f"closed_trades: {result.get('closed_trade_count', 0)} win_rate={result.get('win_rate', 0)}")
            elif result.get("action") == "alerts":
                print(f"alerts: {result.get('alert_count', 0)} status={result.get('status', 'ok')}")
        elif result.get("command") == "backtest":
            print(f"backtest {result.get('action')}: {result.get('status', 'ok')}")
            if result.get("action") == "history":
                print(f"item_count: {result.get('item_count', 0)}")
                print(f"index_path: {result.get('index_path', '')}")
            elif result.get("action") == "compare":
                print(f"item_count: {result.get('item_count', 0)}")
                leaders = result.get("leaders", {})
                print(
                    "leaders: "
                    f"best_pnl={leaders.get('best_pnl', {}).get('total_realized_pnl', 0)} "
                    f"best_win_rate={leaders.get('best_win_rate', {}).get('win_rate', 0)} "
                    f"largest_sample={leaders.get('largest_sample', {}).get('sample_count', 0)}"
                )
            else:
                print(f"sample_count: {result.get('sample_count', 0)}")
                print(
                    "score_summary: "
                    f"win_rate={result.get('score_summary', {}).get('win_rate', result.get('score_summary', {}).get('mean_win_rate', 0))} "
                    f"pnl={result.get('score_summary', {}).get('total_realized_pnl', 0)}"
                )
                if result.get("report_path"):
                    print(f"report_path: {result.get('report_path')}")
        elif result.get("command") == "orchestrate":
            print(f"workflow {result['workflow']}: {result['status']}")
            print(f"steps: {', '.join(step['step'] for step in result.get('steps', []))}")
        elif result.get("command") == "order":
            action = result.get("action", "")
            print(f"order {action}: {result.get('status', 'ok')}")
            if action == "pending":
                print(f"pending_count: {result.get('pending_count', 0)}")
                for item in result.get("items", []):
                    print(f"  {item.get('name', '')} {item.get('type', '')} @ {item.get('currency', '¥')}{item.get('price', 0):.2f}")
            elif action == "confirm":
                print(f"reply: {result.get('reply', {}).get('raw', '')}")
                print(f"matched_order_count: {result.get('matched_order_count', 0)}")
                print(f"trade_event_recorded: {result.get('trade_event_recorded', False)}")
            elif action == "remind":
                print(f"pending_count: {result.get('pending_count', 0)}")
                print(f"send: {result.get('send', False)} discord_ok={result.get('discord_ok', False)}")
            elif action == "overdue-check":
                print(f"t1_remind: {result.get('t1_remind_count', 0)} t2_exception: {result.get('t2_exception_count', 0)}")
            elif action == "place":
                order = result.get("order", {})
                print(f"placed: {order.get('code', '')} {order.get('name', '')} {order.get('condition_type', '')} @ ¥{order.get('trigger_price', 0):.2f}")
            elif action == "cancel":
                print(f"cancelled: {result.get('cancelled_count', 0)} orders for {result.get('code', '')}")
            elif action == "modify":
                print(f"modified: {result.get('modified_count', 0)} orders for {result.get('code', '')} → ¥{result.get('new_price', 0):.2f}")
            elif action == "list":
                print(f"orders: {result.get('order_count', 0)}")
        else:
            print(f"run {result['pipeline']}: {result['status']}")
            print(f"run_id: {result['run_id']}")
            print(f"result_path: {result['result_path']}")


if __name__ == "__main__":
    main()
