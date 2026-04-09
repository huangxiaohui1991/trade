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
from scripts.pipeline.core_pool_scoring import run as run_scoring
from scripts.pipeline.evening import run as run_evening
from scripts.pipeline.morning import run as run_morning
from scripts.pipeline.noon import run as run_noon
from scripts.pipeline.stock_screener import run as run_screener
from scripts.pipeline.weekly_review import run as run_weekly
from scripts.state import (
    AUTOMATED_RULES,
    LEDGER_DB_PATH,
    audit_state,
    bootstrap_state,
    load_market_snapshot,
    load_pool_snapshot,
    load_portfolio_snapshot,
    sync_activity_state,
    sync_portfolio_state,
)
from scripts.utils.cache import CACHE_DIR
from scripts.utils.config_loader import get_notification, get_strategy
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
}

PIPELINE_ALIASES = {
    "stock_screener": "screener",
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
        state_audit = audit_state()
        checks["state_audit"] = {
            "ok": state_audit.get("status") == "ok",
            "status": state_audit.get("status", "drift"),
            "snapshot_date": state_audit.get("snapshot_date", ""),
            "checks": state_audit.get("checks", {}),
        }
    except Exception as e:
        checks["state_audit"] = {"ok": False, "error": str(e)}

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
    else:
        result = audit_state()
    return sanitize_for_json({
        "command": "state",
        "action": action,
        "db_path": str(LEDGER_DB_PATH),
        **result,
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


def status_today() -> dict:
    today = load_daily_state()
    strategy = get_strategy()
    today_decision = build_today_decision(strategy=strategy)
    portfolio_snapshot = load_portfolio_snapshot(scope="cn_a_system")
    pool_snapshot = load_pool_snapshot()
    pool_sync_state = audit_state()
    market_snapshot = load_market_snapshot()
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
        "market_snapshot": market_snapshot,
        "market_signal": market_snapshot.get("signal", market_snapshot.get("market_signal", "")),
        "market_snapshot_source": {
            "source": market_snapshot.get("source", ""),
            "source_chain": market_snapshot.get("source_chain", []),
            "as_of_date": market_snapshot.get("as_of_date", ""),
        },
        "pool_sync_state": pool_sync_state,
        "rule_automation_scope": AUTOMATED_RULES,
        "pool_management": {
            "updated_at": pool_snapshot.get("updated_at", ""),
            "last_eval_date": pool_snapshot.get("snapshot_date", ""),
            "summary": pool_snapshot.get("summary", {}),
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

    doctor_result = doctor()
    payload["doctor"] = {
        "status": doctor_result.get("status", "error"),
        "hard_fail": doctor_result.get("hard_fail", []),
        "warning": doctor_result.get("warning", []),
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

    status_before = status_today()
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

    status_after = status_today()
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

    state_parser = sub.add_parser("state", help="Manage structured ledger state")
    state_sub = state_parser.add_subparsers(dest="action", required=True)
    state_bootstrap = state_sub.add_parser("bootstrap")
    state_bootstrap.add_argument("--force", action="store_true", help="Rebuild ledger from current markdown/config")
    state_sync = state_sub.add_parser("sync")
    state_sync.add_argument("--target", choices=["portfolio", "activity", "all"], default="all")
    state_sub.add_parser("audit")

    run_parser = sub.add_parser("run", help="Run pipeline")
    run_sub = run_parser.add_subparsers(dest="pipeline", required=True)
    for name in ["morning", "noon", "evening", "scoring", "weekly"]:
        run_sub.add_parser(name)
    screener = run_sub.add_parser("screener")
    screener.add_argument("--pool", choices=["core", "watch", "all"], default="watch")
    screener.add_argument("--universe", choices=["tracked", "market"], default="tracked")

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
                elif args.command == "workflows":
                    result = list_workflows()
                elif args.command == "templates":
                    result = list_agent_templates()
                elif args.command == "run":
                    result = run_pipeline(args.pipeline, args)
                elif args.command == "orchestrate":
                    result = orchestrate_workflow(args.workflow, args)
                elif args.command == "state":
                    result = state_command(args.action, args)
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
        elif args.command == "workflows":
            result = list_workflows()
        elif args.command == "templates":
            result = list_agent_templates()
        elif args.command == "run":
            result = run_pipeline(args.pipeline, args)
        elif args.command == "orchestrate":
            result = orchestrate_workflow(args.workflow, args)
        elif args.command == "state":
            result = state_command(args.action, args)
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
        elif result.get("command") == "orchestrate":
            print(f"workflow {result['workflow']}: {result['status']}")
            print(f"steps: {', '.join(step['step'] for step in result.get('steps', []))}")
        else:
            print(f"run {result['pipeline']}: {result['status']}")
            print(f"run_id: {result['run_id']}")
            print(f"result_path: {result['result_path']}")


if __name__ == "__main__":
    main()
