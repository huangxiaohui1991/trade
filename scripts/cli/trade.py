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


def _json_print(payload: dict):
    print(json.dumps(sanitize_for_json(payload), ensure_ascii=False, indent=2))


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
        if "market_data" in result:
            summary["market_signal"] = result["market_data"].get("market_signal", "")
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


def status_today() -> dict:
    today = load_daily_state()
    strategy = get_strategy()
    today_decision = build_today_decision(strategy=strategy)
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
    }


def main():
    parser = argparse.ArgumentParser(description="Trade system unified CLI")
    parser.add_argument("--json", action="store_true", help="Output JSON")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("doctor", help="Run health checks")

    run_parser = sub.add_parser("run", help="Run pipeline")
    run_sub = run_parser.add_subparsers(dest="pipeline", required=True)
    for name in ["morning", "noon", "evening", "scoring", "weekly"]:
        run_sub.add_parser(name)
    screener = run_sub.add_parser("screener")
    screener.add_argument("--pool", choices=["core", "watch", "all"], default="watch")
    screener.add_argument("--universe", choices=["tracked", "market"], default="tracked")

    status_parser = sub.add_parser("status", help="Show current status")
    status_parser.add_argument("target", choices=["today"])

    args = parser.parse_args()

    if args.json:
        stdout_buf = io.StringIO()
        stderr_buf = io.StringIO()
        previous_disable = logging.root.manager.disable
        logging.disable(logging.CRITICAL)
        set_console_logging(False)
        try:
            with contextlib.redirect_stdout(stdout_buf), contextlib.redirect_stderr(stderr_buf):
                if args.command == "doctor":
                    result = doctor()
                elif args.command == "run":
                    result = run_pipeline(args.pipeline, args)
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
        elif args.command == "run":
            result = run_pipeline(args.pipeline, args)
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
        elif result.get("command") == "status":
            print(f"status today: {result.get('date')}")
            print(f"today_decision: {result.get('today_decision', {}).get('decision')}")
            print("pipelines:", ", ".join(sorted(result.get("pipelines", {}).keys())))
        else:
            print(f"run {result['pipeline']}: {result['status']}")
            print(f"run_id: {result['run_id']}")
            print(f"result_path: {result['result_path']}")


if __name__ == "__main__":
    main()
