"""Minimal backtest / walk-forward skeleton built on structured state."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from scripts.engine.composite import build_today_decision
from scripts.state import audit_state, load_market_snapshot, load_pool_snapshot, load_trade_review
from scripts.utils.config_loader import get_strategy


def _parse_date(value: str) -> date:
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except Exception as exc:  # pragma: no cover - invalid CLI input is surfaced to caller
        raise ValueError(f"invalid date: {value!r}") from exc


def _load_fixture(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return {}
    fixture_path = Path(path)
    return json.loads(fixture_path.read_text(encoding="utf-8"))


def _inclusive_days(start: date, end: date) -> int:
    return max((end - start).days + 1, 1)


def _severity_rank(value: str) -> int:
    order = {"ok": 0, "info": 1, "warning": 2, "drift": 3, "block": 4, "error": 5}
    return order.get(str(value).lower(), 1)


def _status_from_components(*components: str) -> str:
    normalized = [str(component).lower() for component in components if str(component).strip()]
    if any(component == "error" for component in normalized):
        return "error"
    if any(component == "drift" for component in normalized):
        return "drift"
    if any(component in {"warning", "block"} for component in normalized):
        return "warning"
    return "ok"


def _merge_backtest_inputs(
    *,
    start: date,
    end: date,
    scope: str,
    fixture: dict[str, Any] | None = None,
) -> dict[str, Any]:
    fixture = fixture or {}
    window = _inclusive_days(start, end)
    trade_review = fixture.get("trade_review") or fixture.get("trade") or load_trade_review(window=window, scope=scope)
    pool_snapshot = fixture.get("pool_snapshot") or fixture.get("pool") or load_pool_snapshot()
    market_snapshot = fixture.get("market_snapshot") or fixture.get("market") or load_market_snapshot()
    state_audit = fixture.get("state_audit") or audit_state()
    today_decision = fixture.get("today_decision") or build_today_decision(strategy=get_strategy())
    return {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "scope": scope,
        "window_days": window,
        "trade_review": trade_review,
        "pool_snapshot": pool_snapshot,
        "market_snapshot": market_snapshot,
        "state_audit": state_audit,
        "today_decision": today_decision,
        "source_mode": "fixture" if fixture else "structured_state",
        "fixture_path": fixture.get("_fixture_path", ""),
    }


def load_backtest_inputs(
    start: str,
    end: str,
    *,
    scope: str = "cn_a_system",
    fixture: str | Path | None = None,
) -> dict[str, Any]:
    start_date = _parse_date(start)
    end_date = _parse_date(end)
    if end_date < start_date:
        raise ValueError("end must be on or after start")
    fixture_payload = _load_fixture(fixture)
    if fixture:
        fixture_payload["_fixture_path"] = str(Path(fixture))
    return _merge_backtest_inputs(start=start_date, end=end_date, scope=scope, fixture=fixture_payload)


def _summarize_score(inputs: dict[str, Any]) -> dict[str, Any]:
    trade_review = inputs.get("trade_review", {}) or {}
    pool_snapshot = inputs.get("pool_snapshot", {}) or {}
    market_snapshot = inputs.get("market_snapshot", {}) or {}
    closed_trade_count = int(trade_review.get("closed_trade_count", 0) or 0)
    total_realized_pnl = float(trade_review.get("total_realized_pnl", 0.0) or 0.0)
    pool_summary = pool_snapshot.get("summary", {}) if isinstance(pool_snapshot, dict) else {}
    return {
        "closed_trade_count": closed_trade_count,
        "win_count": int(trade_review.get("win_count", 0) or 0),
        "loss_count": int(trade_review.get("loss_count", 0) or 0),
        "win_rate": float(trade_review.get("win_rate", 0.0) or 0.0),
        "total_realized_pnl": round(total_realized_pnl, 2),
        "average_realized_pnl": round(total_realized_pnl / closed_trade_count, 2) if closed_trade_count else 0.0,
        "pool_core_count": int(pool_summary.get("core_count", len(pool_snapshot.get("core_pool", []))) or 0),
        "pool_watch_count": int(pool_summary.get("watch_count", len(pool_snapshot.get("watch_pool", []))) or 0),
        "market_signal": str(market_snapshot.get("signal", market_snapshot.get("market_signal", ""))).strip().upper(),
    }


def _summarize_risk(inputs: dict[str, Any]) -> dict[str, Any]:
    state_audit = inputs.get("state_audit", {}) or {}
    today_decision = inputs.get("today_decision", {}) or {}
    trade_review = inputs.get("trade_review", {}) or {}
    portfolio_risk = today_decision.get("portfolio_risk", {}) or {}
    risk_state = _status_from_components(
        str(state_audit.get("status", "ok")),
        str(portfolio_risk.get("state", "ok")),
        str(trade_review.get("mfe_mae_status", "ok")),
    )
    return {
        "risk_state": risk_state,
        "state_audit_status": state_audit.get("status", "drift"),
        "pool_sync_state": state_audit.get("status", "drift"),
        "portfolio_risk_state": portfolio_risk.get("state", ""),
        "portfolio_risk_reason_codes": portfolio_risk.get("reason_codes", []),
        "portfolio_risk_reasons": portfolio_risk.get("reasons", []),
        "mfe_mae_status": trade_review.get("mfe_mae_status", ""),
        "open_position_count": int(trade_review.get("open_position_count", 0) or 0),
    }


def _state_fields(inputs: dict[str, Any]) -> dict[str, Any]:
    state_audit = inputs.get("state_audit", {}) or {}
    market_snapshot = inputs.get("market_snapshot", {}) or {}
    trade_review = inputs.get("trade_review", {}) or {}
    today_decision = inputs.get("today_decision", {}) or {}
    return {
        "market": {
            "signal": market_snapshot.get("signal", market_snapshot.get("market_signal", "")),
            "as_of_date": market_snapshot.get("as_of_date", ""),
            "source": market_snapshot.get("source", ""),
        },
        "pool": {
            "status": state_audit.get("status", "drift"),
            "snapshot_date": state_audit.get("snapshot_date", ""),
        },
        "trade": {
            "closed_trade_count": trade_review.get("closed_trade_count", 0),
            "open_position_count": trade_review.get("open_position_count", 0),
            "mfe_mae_status": trade_review.get("mfe_mae_status", ""),
        },
        "decision": {
            "action": today_decision.get("action", today_decision.get("decision", "")),
            "portfolio_risk_state": (today_decision.get("portfolio_risk", {}) or {}).get("state", ""),
        },
    }


def run_backtest(
    start: str,
    end: str,
    *,
    scope: str = "cn_a_system",
    fixture: str | Path | None = None,
) -> dict[str, Any]:
    inputs = load_backtest_inputs(start, end, scope=scope, fixture=fixture)
    score_summary = _summarize_score(inputs)
    risk_summary = _summarize_risk(inputs)
    sample_count = int(score_summary["closed_trade_count"])
    status = _status_from_components(
        str(inputs.get("state_audit", {}).get("status", "ok")),
        str(score_summary.get("market_signal", "ok")),
        str(risk_summary.get("risk_state", "ok")),
    )
    if sample_count == 0:
        status = "warning" if status == "ok" else status
    return {
        "command": "backtest",
        "action": "run",
        "status": status,
        "parameters": {
            "start": inputs["start"],
            "end": inputs["end"],
            "scope": inputs["scope"],
            "window_days": inputs["window_days"],
            "fixture": inputs.get("fixture_path", ""),
            "source_mode": inputs["source_mode"],
        },
        "sample_count": sample_count,
        "score_summary": score_summary,
        "risk_summary": risk_summary,
        "state_fields": _state_fields(inputs),
    }


def _walk_forward_windows(start: date, end: date, folds: int) -> list[dict[str, str]]:
    span_days = _inclusive_days(start, end)
    fold_count = max(1, min(int(folds or 1), max(1, span_days - 1)))
    if fold_count == 1:
        return [{
            "train_start": start.isoformat(),
            "train_end": start.isoformat(),
            "test_start": start.isoformat(),
            "test_end": end.isoformat(),
        }]

    test_span = max(1, span_days // (fold_count + 1))
    windows: list[dict[str, str]] = []
    for index in range(fold_count):
        train_end_offset = max(0, min(span_days - 2, (span_days * (index + 1)) // (fold_count + 1) - 1))
        test_start_offset = train_end_offset + 1
        test_end_offset = min(span_days - 1, test_start_offset + test_span - 1)
        windows.append({
            "train_start": start.isoformat(),
            "train_end": (start + timedelta(days=train_end_offset)).isoformat(),
            "test_start": (start + timedelta(days=test_start_offset)).isoformat(),
            "test_end": (start + timedelta(days=test_end_offset)).isoformat(),
        })
    return windows


def run_walk_forward(
    start: str,
    end: str,
    *,
    scope: str = "cn_a_system",
    folds: int = 3,
    fixture: str | Path | None = None,
) -> dict[str, Any]:
    start_date = _parse_date(start)
    end_date = _parse_date(end)
    if end_date < start_date:
        raise ValueError("end must be on or after start")

    inputs = load_backtest_inputs(start, end, scope=scope, fixture=fixture)
    windows = _walk_forward_windows(start_date, end_date, folds)
    fold_reports = []
    for index, window in enumerate(windows, start=1):
        fold_inputs = dict(inputs)
        fold_inputs["start"] = window["train_start"]
        fold_inputs["end"] = window["test_end"]
        fold_score = _summarize_score(fold_inputs)
        fold_risk = _summarize_risk(fold_inputs)
        fold_reports.append({
            "fold": index,
            "train_start": window["train_start"],
            "train_end": window["train_end"],
            "test_start": window["test_start"],
            "test_end": window["test_end"],
            "sample_count": int(fold_score["closed_trade_count"]),
            "score_summary": fold_score,
            "risk_summary": fold_risk,
            "status": _status_from_components(
                str(fold_risk.get("risk_state", "ok")),
                str(fold_score.get("market_signal", "ok")),
            ),
        })

    total_sample_count = sum(item["sample_count"] for item in fold_reports)
    score_summary = {
        "fold_count": len(fold_reports),
        "mean_win_rate": round(
            sum(item["score_summary"]["win_rate"] for item in fold_reports) / len(fold_reports), 1
        ) if fold_reports else 0.0,
        "total_realized_pnl": round(
            sum(item["score_summary"]["total_realized_pnl"] for item in fold_reports), 2
        ),
        "mean_sample_count": round(total_sample_count / len(fold_reports), 1) if fold_reports else 0.0,
    }
    risk_summary = {
        "fold_count": len(fold_reports),
        "worst_risk_state": max((item["risk_summary"]["risk_state"] for item in fold_reports), key=_severity_rank, default="ok"),
        "worst_pool_sync_state": max((item["risk_summary"]["pool_sync_state"] for item in fold_reports), key=_severity_rank, default="ok"),
    }
    overall_status = max((item["status"] for item in fold_reports), key=_severity_rank, default="ok")
    if total_sample_count == 0 and overall_status == "ok":
        overall_status = "warning"
    return {
        "command": "backtest",
        "action": "walk-forward",
        "status": overall_status,
        "parameters": {
            "start": start_date.isoformat(),
            "end": end_date.isoformat(),
            "scope": scope,
            "folds": len(fold_reports),
            "fixture": str(fixture) if fixture else "",
            "source_mode": inputs["source_mode"],
        },
        "sample_count": total_sample_count,
        "score_summary": score_summary,
        "risk_summary": risk_summary,
        "state_fields": _state_fields(inputs),
        "folds": fold_reports,
    }
