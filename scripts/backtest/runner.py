"""Minimal backtest / walk-forward skeleton built on structured state."""

from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from scripts.engine.composite import build_today_decision
from scripts.state import audit_state, load_market_snapshot, load_pool_snapshot, load_trade_review
from scripts.utils.config_loader import get_strategy

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
BACKTEST_SAMPLE_DIR = PROJECT_ROOT / "data" / "backtest" / "samples"
BACKTEST_REPORT_DIR = PROJECT_ROOT / "data" / "backtest" / "reports"
BACKTEST_INDEX_PATH = PROJECT_ROOT / "data" / "backtest" / "index.json"


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


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _date_in_range(value: str, start: date, end: date) -> bool:
    try:
        current = datetime.strptime(str(value), "%Y-%m-%d").date()
    except Exception:
        return False
    return start <= current <= end


def _extract_entry_score(trade: dict[str, Any]) -> float | None:
    for key in ("entry_score", "score", "total_score"):
        if key in trade and trade.get(key) not in (None, ""):
            return _safe_float(trade.get(key), 0.0)

    texts = [
        str(trade.get("entry_reason_text", "")).strip(),
        str((trade.get("metadata", {}) or {}).get("entry_reason_text", "")).strip(),
    ]
    pattern = re.compile(r"评分\s*([0-9]+(?:\.[0-9]+)?)")
    for text in texts:
        if not text:
            continue
        match = pattern.search(text)
        if match:
            return _safe_float(match.group(1), 0.0)
    return None


def _recompute_weighted_score(trade: dict[str, Any], params: dict[str, float]) -> float | None:
    components = [
        ("technical_score", "technical_weight", 2.0),
        ("fundamental_score", "fundamental_weight", 3.0),
        ("flow_score", "flow_weight", 2.0),
        ("sentiment_score", "sentiment_weight", 3.0),
    ]
    if not any(trade.get(score_key) not in (None, "") for score_key, _, _ in components):
        return None

    total = 0.0
    for score_key, weight_key, denom in components:
        score_value = trade.get(score_key)
        if score_value in (None, ""):
            continue
        total += _safe_float(score_value, 0.0) * _safe_float(params.get(weight_key, denom), denom) / denom
    return round(total, 2)


def _baseline_parameters() -> dict[str, float]:
    strategy = get_strategy()
    scoring_cfg = strategy.get("scoring", {})
    risk_cfg = strategy.get("risk", {})
    return {
        "buy_threshold": _safe_float(scoring_cfg.get("thresholds", {}).get("buy", 7), 7.0),
        "stop_loss": _safe_float(risk_cfg.get("stop_loss", 0.04), 0.04),
        "take_profit": _safe_float(risk_cfg.get("take_profit", {}).get("t1_pct", 0.15), 0.15),
    }


def _coerce_grid(values: str | list[float] | None, default: list[float]) -> list[float]:
    if values is None:
        return default
    if isinstance(values, str):
        parsed = []
        for item in values.split(","):
            item = item.strip()
            if not item:
                continue
            parsed.append(_safe_float(item))
        return parsed or default
    parsed = [_safe_float(item) for item in values if item not in (None, "")]
    return parsed or default


def _parameter_grid(
    *,
    buy_thresholds: str | list[float] | None = None,
    stop_losses: str | list[float] | None = None,
    take_profits: str | list[float] | None = None,
    technical_weights: str | list[float] | None = None,
    fundamental_weights: str | list[float] | None = None,
    flow_weights: str | list[float] | None = None,
    sentiment_weights: str | list[float] | None = None,
) -> list[dict[str, float]]:
    baseline = _baseline_parameters()
    strategy = get_strategy()
    scoring_weights = strategy.get("scoring", {}).get("weights", {})
    buy_grid = _coerce_grid(
        buy_thresholds,
        sorted({baseline["buy_threshold"] - 1.0, baseline["buy_threshold"], baseline["buy_threshold"] + 1.0}),
    )
    stop_grid = _coerce_grid(
        stop_losses,
        sorted({
            max(0.01, round(baseline["stop_loss"] - 0.01, 3)),
            round(baseline["stop_loss"], 3),
            round(baseline["stop_loss"] + 0.01, 3),
        }),
    )
    take_profit_grid = _coerce_grid(
        take_profits,
        sorted({
            max(0.02, round(baseline["take_profit"] - 0.05, 3)),
            round(baseline["take_profit"], 3),
            round(baseline["take_profit"] + 0.05, 3),
        }),
    )
    technical_grid = _coerce_grid(
        technical_weights,
        [round(_safe_float(scoring_weights.get("technical", 2), 2), 2)],
    )
    fundamental_grid = _coerce_grid(
        fundamental_weights,
        [round(_safe_float(scoring_weights.get("fundamental", 3), 2), 2)],
    )
    flow_grid = _coerce_grid(
        flow_weights,
        [round(_safe_float(scoring_weights.get("flow", 2), 2), 2)],
    )
    sentiment_grid = _coerce_grid(
        sentiment_weights,
        [round(_safe_float(scoring_weights.get("sentiment", 3), 2), 2)],
    )
    grid: list[dict[str, float]] = []
    for buy_threshold in buy_grid:
        for stop_loss in stop_grid:
            for take_profit in take_profit_grid:
                for technical_weight in technical_grid:
                    for fundamental_weight in fundamental_grid:
                        for flow_weight in flow_grid:
                            for sentiment_weight in sentiment_grid:
                                grid.append({
                                    "buy_threshold": round(_safe_float(buy_threshold), 3),
                                    "stop_loss": round(_safe_float(stop_loss), 3),
                                    "take_profit": round(_safe_float(take_profit), 3),
                                    "technical_weight": round(_safe_float(technical_weight), 3),
                                    "fundamental_weight": round(_safe_float(fundamental_weight), 3),
                                    "flow_weight": round(_safe_float(flow_weight), 3),
                                    "sentiment_weight": round(_safe_float(sentiment_weight), 3),
                                })
    return grid


def _filter_closed_trades(trade_review: dict[str, Any], start: date, end: date) -> list[dict[str, Any]]:
    return [
        dict(item)
        for item in trade_review.get("closed_trades", [])
        if _date_in_range(str(item.get("exit_date", "")), start, end)
    ]


def _build_history_samples(
    trades: list[dict[str, Any]],
    *,
    scope: str,
    start: str,
    end: str,
    source_mode: str,
) -> dict[str, Any]:
    generated_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    items = []
    for trade in trades:
        items.append({
            "code": str(trade.get("code", "")).strip(),
            "name": str(trade.get("name", "")).strip(),
            "entry_date": str(trade.get("entry_date", "")).strip(),
            "exit_date": str(trade.get("exit_date", "")).strip(),
            "entry_price": _safe_float(trade.get("entry_price"), 0.0),
            "exit_price": _safe_float(trade.get("exit_price"), 0.0),
            "realized_pnl": _safe_float(trade.get("realized_pnl"), 0.0),
            "mfe_pct": trade.get("mfe_pct"),
            "mae_pct": trade.get("mae_pct"),
            "excursion_source": trade.get("excursion_source", "proxy_market_history"),
            "exit_reason_codes": list(trade.get("exit_reason_codes", [])),
        })
    return {
        "scope": scope,
        "start": start,
        "end": end,
        "source_mode": source_mode,
        "generated_at": generated_at,
        "sample_count": len(items),
        "samples": items,
    }


def _persist_history_samples(payload: dict[str, Any]) -> str:
    BACKTEST_SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    scope = str(payload.get("scope", "unknown")).strip() or "unknown"
    start = str(payload.get("start", "")).strip() or "unknown"
    end = str(payload.get("end", "")).strip() or "unknown"
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = BACKTEST_SAMPLE_DIR / f"{scope}_{start}_{end}_{stamp}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)


def _build_artifacts(result_path: str, report_path: str, sample_path: str) -> list[dict[str, str]]:
    artifacts = []
    if report_path:
        artifacts.append({"type": "report", "path": report_path})
    if result_path:
        artifacts.append({"type": "result", "path": result_path})
    if sample_path:
        artifacts.append({"type": "sample_store", "path": sample_path})
    return artifacts


def _render_backtest_report(action: str, payload: dict[str, Any]) -> str:
    parameters = payload.get("parameters", {}) or {}
    score_summary = payload.get("score_summary", {}) or {}
    risk_summary = payload.get("risk_summary", {}) or {}
    sample_store = payload.get("sample_store", {}) or {}
    comparison_summary = payload.get("comparison_summary", {}) or {}
    lines = [
        f"# Backtest {action}",
        "",
        "## Summary",
        "",
        f"- Status: {payload.get('status', 'ok')}",
        f"- Scope: {parameters.get('scope', '')}",
        f"- Window: {parameters.get('start', '')} -> {parameters.get('end', '')}",
        f"- Engine: {parameters.get('engine_mode', '')}",
        f"- Source: {parameters.get('source_mode', '')}",
        f"- Sample Count: {payload.get('sample_count', 0)}",
        "",
        "## Score",
        "",
        f"- Total Realized PnL: {score_summary.get('total_realized_pnl', 0)}",
    ]

    if "win_rate" in score_summary:
        lines.append(f"- Win Rate: {score_summary.get('win_rate', 0)}")
    if "mean_win_rate" in score_summary:
        lines.append(f"- Mean Win Rate: {score_summary.get('mean_win_rate', 0)}")
    if "selected_parameters" in payload:
        lines.extend([
            "",
            "## Selected Parameters",
            "",
            f"- Buy Threshold: {(payload.get('selected_parameters', {}) or {}).get('buy_threshold', '')}",
            f"- Stop Loss: {(payload.get('selected_parameters', {}) or {}).get('stop_loss', '')}",
            f"- Take Profit: {(payload.get('selected_parameters', {}) or {}).get('take_profit', '')}",
        ])
    elif "selected_parameters" in score_summary:
        lines.extend([
            "",
            "## Selected Parameters",
            "",
            f"- Buy Threshold: {(score_summary.get('selected_parameters', {}) or {}).get('buy_threshold', '')}",
            f"- Stop Loss: {(score_summary.get('selected_parameters', {}) or {}).get('stop_loss', '')}",
            f"- Take Profit: {(score_summary.get('selected_parameters', {}) or {}).get('take_profit', '')}",
        ])

    if action.replace("_", "-") == "walk-forward" and comparison_summary:
        rows = comparison_summary.get("rows", []) or []
        lines.extend([
            "",
            "## Fold Comparison",
            "",
        ])
        if rows:
            lines.extend([
                _markdown_table(
                    [
                        "Fold",
                        "Train Window",
                        "Test Window",
                        "Selected Parameters",
                        "Train PnL",
                        "Train Win Rate",
                        "Train Samples",
                        "Eval PnL",
                        "Eval Win Rate",
                        "Eval Samples",
                    ],
                    [
                        [
                            row.get("fold", ""),
                            row.get("train_window", ""),
                            row.get("test_window", ""),
                            row.get("selected_parameters", ""),
                            row.get("training_pnl", ""),
                            row.get("training_win_rate", ""),
                            row.get("training_sample_count", ""),
                            row.get("evaluation_pnl", ""),
                            row.get("evaluation_win_rate", ""),
                            row.get("evaluation_sample_count", ""),
                        ]
                        for row in rows
                    ],
                ),
                "",
                "## Aggregate Comparison",
                "",
                _markdown_table(
                    ["Metric", "Train", "Eval", "Delta"],
                    [
                        [
                            "Realized PnL",
                            comparison_summary.get("training_total_realized_pnl", 0),
                            comparison_summary.get("evaluation_total_realized_pnl", 0),
                            comparison_summary.get("train_eval_pnl_delta", 0),
                        ],
                        [
                            "Win Rate",
                            comparison_summary.get("training_mean_win_rate", 0),
                            comparison_summary.get("evaluation_mean_win_rate", 0),
                            comparison_summary.get("train_eval_win_rate_delta", 0),
                        ],
                        [
                            "Sample Count",
                            comparison_summary.get("training_mean_sample_count", 0),
                            comparison_summary.get("evaluation_mean_sample_count", 0),
                            comparison_summary.get("train_eval_sample_count_delta", 0),
                        ],
                    ],
                ),
                "",
                f"- Best Eval Fold: {((comparison_summary.get('best_eval_fold', {}) or {}).get('fold', ''))}",
                f"- Best Train Fold: {((comparison_summary.get('best_train_fold', {}) or {}).get('fold', ''))}",
            ])

    lines.extend([
        "",
        "## Risk",
        "",
        f"- Risk State: {risk_summary.get('risk_state', risk_summary.get('worst_risk_state', ''))}",
        f"- Pool Sync State: {risk_summary.get('pool_sync_state', risk_summary.get('worst_pool_sync_state', ''))}",
        f"- Excursion Source: {risk_summary.get('mfe_mae_status', '')}",
        "",
        "## Artifacts",
        "",
        f"- Sample Store Count: {sample_store.get('sample_count', 0)}",
        f"- Sample Store Path: {sample_store.get('path', '')}",
    ])
    return "\n".join(lines) + "\n"


def _persist_backtest_outputs(action: str, payload: dict[str, Any]) -> tuple[str, str]:
    BACKTEST_REPORT_DIR.mkdir(parents=True, exist_ok=True)
    parameters = payload.get("parameters", {}) or {}
    scope = str(parameters.get("scope", "unknown")).strip() or "unknown"
    start = str(parameters.get("start", "")).strip() or "unknown"
    end = str(parameters.get("end", "")).strip() or "unknown"
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    stem = f"{action}_{scope}_{start}_{end}_{stamp}"
    result_path = BACKTEST_REPORT_DIR / f"{stem}.json"
    report_path = BACKTEST_REPORT_DIR / f"{stem}.md"
    report_path.write_text(_render_backtest_report(action, payload), encoding="utf-8")
    result_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(result_path), str(report_path)


def _update_backtest_index(action: str, payload: dict[str, Any], result_path: str, report_path: str, sample_path: str) -> None:
    BACKTEST_INDEX_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        items = json.loads(BACKTEST_INDEX_PATH.read_text(encoding="utf-8"))
        if not isinstance(items, list):
            items = []
    except Exception:
        items = []
    parameters = payload.get("parameters", {}) or {}
    score_summary = payload.get("score_summary", {}) or {}
    risk_summary = payload.get("risk_summary", {}) or {}
    selected_parameters = payload.get("selected_parameters") or score_summary.get("selected_parameters") or {}
    entry = {
        "created_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "command": payload.get("command", "backtest"),
        "action": action,
        "status": payload.get("status", "ok"),
        "scope": parameters.get("scope", ""),
        "start": parameters.get("start", ""),
        "end": parameters.get("end", ""),
        "engine_mode": parameters.get("engine_mode", ""),
        "source_mode": parameters.get("source_mode", ""),
        "sample_count": payload.get("sample_count", 0),
        "total_realized_pnl": score_summary.get("total_realized_pnl", 0),
        "win_rate": score_summary.get("win_rate", score_summary.get("mean_win_rate", 0)),
        "risk_state": risk_summary.get("risk_state", risk_summary.get("worst_risk_state", "")),
        "excursion_source": (payload.get("sample_store", {}) or {}).get("source", ""),
        "selected_parameters": selected_parameters,
        "result_path": result_path,
        "report_path": report_path,
        "sample_store_path": sample_path,
    }
    items.insert(0, entry)
    BACKTEST_INDEX_PATH.write_text(json.dumps(items[:200], ensure_ascii=False, indent=2), encoding="utf-8")


def list_backtest_history(*, limit: int = 10) -> dict[str, Any]:
    try:
        items = json.loads(BACKTEST_INDEX_PATH.read_text(encoding="utf-8"))
        if not isinstance(items, list):
            items = []
    except Exception:
        items = []
    normalized_limit = max(1, int(limit or 10))
    return {
        "command": "backtest",
        "action": "history",
        "status": "ok" if items else "warning",
        "index_path": str(BACKTEST_INDEX_PATH),
        "item_count": len(items[:normalized_limit]),
        "items": items[:normalized_limit],
    }


def compare_backtest_history(*, limit: int = 20) -> dict[str, Any]:
    history = list_backtest_history(limit=max(1, int(limit or 20)))
    items = list(history.get("items", []))
    if not items:
        return {
            "command": "backtest",
            "action": "compare",
            "status": "warning",
            "index_path": history.get("index_path", str(BACKTEST_INDEX_PATH)),
            "item_count": 0,
            "summary": {
                "status_counts": {},
                "action_counts": {},
                "engine_counts": {},
                "excursion_source_counts": {},
                "scope_counts": {},
            },
            "leaders": {},
            "items": [],
        }

    def _pick_best(key: str, reverse: bool = True) -> dict[str, Any]:
        ranked = sorted(items, key=lambda item: _safe_float(item.get(key, 0), 0.0), reverse=reverse)
        return ranked[0] if ranked else {}

    status_counts: dict[str, int] = {}
    action_counts: dict[str, int] = {}
    engine_counts: dict[str, int] = {}
    excursion_source_counts: dict[str, int] = {}
    scope_counts: dict[str, int] = {}
    for item in items:
        status = str(item.get("status", "")).strip() or "unknown"
        action = str(item.get("action", "")).strip() or "unknown"
        engine = str(item.get("engine_mode", "")).strip() or "unknown"
        excursion_source = str(item.get("excursion_source", "")).strip() or "unknown"
        scope = str(item.get("scope", "")).strip() or "unknown"
        status_counts[status] = status_counts.get(status, 0) + 1
        action_counts[action] = action_counts.get(action, 0) + 1
        engine_counts[engine] = engine_counts.get(engine, 0) + 1
        excursion_source_counts[excursion_source] = excursion_source_counts.get(excursion_source, 0) + 1
        scope_counts[scope] = scope_counts.get(scope, 0) + 1

    return {
        "command": "backtest",
        "action": "compare",
        "status": "ok",
        "index_path": history.get("index_path", str(BACKTEST_INDEX_PATH)),
        "item_count": len(items),
        "summary": {
            "status_counts": status_counts,
            "action_counts": action_counts,
            "engine_counts": engine_counts,
            "excursion_source_counts": excursion_source_counts,
            "scope_counts": scope_counts,
        },
        "leaders": {
            "best_pnl": _pick_best("total_realized_pnl"),
            "best_win_rate": _pick_best("win_rate"),
            "largest_sample": _pick_best("sample_count"),
        },
        "items": items,
    }


def _trade_reason_codes(trade: dict[str, Any]) -> list[str]:
    return [str(item).strip().upper() for item in trade.get("exit_reason_codes", []) if str(item).strip()]


def _estimate_trade_shares(trade: dict[str, Any]) -> int:
    shares = int(trade.get("shares", 0) or 0)
    if shares > 0:
        return shares
    entry_price = _safe_float(trade.get("entry_price", 0.0), 0.0)
    exit_price = _safe_float(trade.get("exit_price", entry_price), entry_price)
    realized_pnl = _safe_float(trade.get("realized_pnl", 0.0), 0.0)
    diff = exit_price - entry_price
    if entry_price <= 0 or abs(diff) < 1e-9 or abs(realized_pnl) < 1e-9:
        return 0
    return max(int(round(abs(realized_pnl / diff))), 0)


def _load_trade_history_for_replay(trade: dict[str, Any]) -> list[dict[str, Any]]:
    fixture_rows = trade.get("history_rows")
    if isinstance(fixture_rows, list) and fixture_rows:
        return [row for row in fixture_rows if isinstance(row, dict)]

    try:
        from scripts.state.service import _load_trade_history_rows
    except Exception:
        return []

    code = str(trade.get("code", "")).strip()
    entry_date = str(trade.get("entry_date", "")).strip()
    exit_date = str(trade.get("exit_date", "")).strip()
    if not code or not entry_date or not exit_date:
        return []
    try:
        rows = _load_trade_history_rows(code, entry_date, exit_date)
    except Exception:
        return []
    return [row for row in rows if isinstance(row, dict)]


def _simulate_trade_with_history(trade: dict[str, Any], params: dict[str, float]) -> tuple[float | None, str]:
    entry_price = _safe_float(trade.get("entry_price", 0.0), 0.0)
    if entry_price <= 0:
        return None, ""

    shares = _estimate_trade_shares(trade)
    if shares <= 0:
        return None, ""

    rows = _load_trade_history_for_replay(trade)
    if not rows:
        return None, ""

    stop_price = round(entry_price * (1 - max(_safe_float(params.get("stop_loss", 0.0), 0.0), 0.0)), 4)
    take_price = round(entry_price * (1 + max(_safe_float(params.get("take_profit", 0.0), 0.0), 0.0)), 4)
    fallback_exit = _safe_float(trade.get("exit_price", entry_price), entry_price)

    for row in rows:
        low_price = _safe_float(row.get("最低", row.get("low", row.get("Low", 0.0))), 0.0)
        high_price = _safe_float(row.get("最高", row.get("high", row.get("High", 0.0))), 0.0)
        if low_price > 0 and low_price <= stop_price:
            return round((stop_price - entry_price) * shares, 2), "historical_stop_loss"
        if high_price > 0 and high_price >= take_price:
            return round((take_price - entry_price) * shares, 2), "historical_take_profit"

    return round((fallback_exit - entry_price) * shares, 2), "historical_hold_to_exit"


def _apply_parameter_set(
    trades: list[dict[str, Any]],
    params: dict[str, float],
    baseline: dict[str, float],
) -> list[dict[str, Any]]:
    evaluated: list[dict[str, Any]] = []
    for trade in trades:
        entry_score = _recompute_weighted_score(trade, params)
        if entry_score is None:
            entry_score = _extract_entry_score(trade)
        if entry_score is not None and entry_score < params["buy_threshold"]:
            continue

        adjusted = dict(trade)
        realized_pnl = _safe_float(trade.get("realized_pnl", 0.0), 0.0)
        reason_codes = _trade_reason_codes(trade)
        adjustment_note = "unchanged"
        replay_pnl, replay_note = _simulate_trade_with_history(trade, params)

        if replay_pnl is not None:
            realized_pnl = replay_pnl
            adjustment_note = replay_note
        elif any("STOP_LOSS" in code for code in reason_codes) and realized_pnl < 0:
            base = max(baseline["stop_loss"], 0.001)
            multiplier = min(max(params["stop_loss"] / base, 0.4), 1.6)
            realized_pnl = round(realized_pnl * multiplier, 2)
            adjustment_note = "stop_loss_proxy"
        elif any("TAKE_PROFIT" in code for code in reason_codes) and realized_pnl > 0:
            base = max(baseline["take_profit"], 0.001)
            multiplier = min(max(params["take_profit"] / base, 0.5), 1.8)
            realized_pnl = round(realized_pnl * multiplier, 2)
            adjustment_note = "take_profit_proxy"

        adjusted["entry_score"] = entry_score
        adjusted["realized_pnl"] = realized_pnl
        adjusted["parameter_adjustment"] = adjustment_note
        evaluated.append(adjusted)
    return evaluated


def _summarize_trade_list(trades: list[dict[str, Any]]) -> dict[str, Any]:
    closed_trade_count = len(trades)
    winners = [item for item in trades if _safe_float(item.get("realized_pnl", 0.0), 0.0) > 0]
    losers = [item for item in trades if _safe_float(item.get("realized_pnl", 0.0), 0.0) < 0]
    total_realized_pnl = round(sum(_safe_float(item.get("realized_pnl", 0.0), 0.0) for item in trades), 2)
    return {
        "closed_trade_count": closed_trade_count,
        "win_count": len(winners),
        "loss_count": len(losers),
        "win_rate": round((len(winners) / closed_trade_count) * 100, 1) if closed_trade_count else 0.0,
        "total_realized_pnl": total_realized_pnl,
        "average_realized_pnl": round(total_realized_pnl / closed_trade_count, 2) if closed_trade_count else 0.0,
    }


def _iter_dates(start: date, end: date) -> list[date]:
    current = start
    items: list[date] = []
    while current <= end:
        items.append(current)
        current += timedelta(days=1)
    return items


def _allocation_priority_score(trade: dict[str, Any]) -> float:
    for key in ("entry_score", "score", "total_score"):
        if trade.get(key) not in (None, ""):
            return round(_safe_float(trade.get(key), 0.0), 4)
    return 0.0


def _build_portfolio_replay(
    trades: list[dict[str, Any]],
    *,
    start: date,
    end: date,
    total_capital: float,
    strategy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_capital = max(_safe_float(total_capital, 0.0), 1.0)
    strategy = strategy or {}
    position_cfg = (strategy.get("risk", {}) or {}).get("position", {}) or {}
    total_exposure_max = max(min(_safe_float(position_cfg.get("total_max", 0.60), 0.60), 1.0), 0.0)
    single_position_max = max(min(_safe_float(position_cfg.get("single_max", 0.20), 0.20), 1.0), 0.0)
    total_cap_limit = round(normalized_capital * total_exposure_max, 2)
    single_cap_limit = round(normalized_capital * single_position_max, 2)
    timeline = []
    peak_exposure = 0.0
    max_positions = 0
    max_capital_deployed = 0.0
    cumulative_realized_pnl = 0.0
    cash_available = round(normalized_capital, 2)
    min_cash_available = round(normalized_capital, 2)
    constrained_trade_count = 0
    rejected_trade_count = 0
    allocation_rank_by_date: dict[str, int] = {}
    accepted_trade_details: list[dict[str, Any]] = []
    rejected_trade_details: list[dict[str, Any]] = []

    candidate_trades = []
    for trade in trades:
        entry_date = str(trade.get("entry_date", "")).strip()
        exit_date = str(trade.get("exit_date", "")).strip()
        entry_price = _safe_float(trade.get("entry_price", 0.0), 0.0)
        shares = _estimate_trade_shares(trade)
        if not entry_date or not exit_date or entry_price <= 0 or shares <= 0:
            continue
        desired_capital = round(entry_price * shares, 2)
        candidate_trades.append({
            "code": str(trade.get("code", "")).strip(),
            "entry_date": entry_date,
            "exit_date": exit_date,
            "desired_capital": desired_capital,
            "realized_pnl": round(_safe_float(trade.get("realized_pnl", 0.0), 0.0), 2),
            "priority_score": _allocation_priority_score(trade),
        })

    candidate_trades.sort(
        key=lambda item: (
            item["entry_date"],
            -_safe_float(item.get("priority_score", 0.0), 0.0),
            -_safe_float(item.get("realized_pnl", 0.0), 0.0),
            _safe_float(item.get("desired_capital", 0.0), 0.0),
            item["exit_date"],
            item.get("code", ""),
        )
    )
    normalized_trades = []
    accepted_positions: list[dict[str, Any]] = []
    for trade in candidate_trades:
        entry_date = trade["entry_date"]
        allocation_rank_by_date[entry_date] = allocation_rank_by_date.get(entry_date, 0) + 1
        allocation_rank = allocation_rank_by_date[entry_date]
        accepted_positions = [item for item in accepted_positions if item["exit_date"] > entry_date]
        current_deployed = round(sum(item["capital"] for item in accepted_positions), 2)
        remaining_total = max(total_cap_limit - current_deployed, 0.0)
        capital = min(trade["desired_capital"], single_cap_limit, remaining_total)
        if capital <= 0:
            rejected_trade_count += 1
            rejected_trade_details.append({
                "code": trade.get("code", ""),
                "entry_date": trade["entry_date"],
                "desired_capital": round(trade["desired_capital"], 2),
                "priority_score": round(_safe_float(trade.get("priority_score", 0.0), 0.0), 4),
                "allocation_rank": allocation_rank,
                "reason": "capital_limit",
            })
            continue
        ratio = capital / trade["desired_capital"] if trade["desired_capital"] > 0 else 0.0
        if ratio < 0.999:
            constrained_trade_count += 1
        accepted = {
            "code": trade.get("code", ""),
            "entry_date": trade["entry_date"],
            "exit_date": trade["exit_date"],
            "capital": round(capital, 2),
            "desired_capital": round(trade["desired_capital"], 2),
            "realized_pnl": round(trade["realized_pnl"] * ratio, 2),
            "priority_score": round(_safe_float(trade.get("priority_score", 0.0), 0.0), 4),
            "allocation_rank": allocation_rank,
        }
        normalized_trades.append(accepted)
        accepted_positions.append(accepted)
        accepted_trade_details.append({
            "code": accepted["code"],
            "entry_date": accepted["entry_date"],
            "capital": accepted["capital"],
            "desired_capital": accepted["desired_capital"],
            "priority_score": accepted["priority_score"],
            "allocation_rank": accepted["allocation_rank"],
        })

    for day in _iter_dates(start, end):
        day_str = day.isoformat()
        exited_today = [item for item in normalized_trades if item["exit_date"] == day_str]
        for item in exited_today:
            cash_available = round(cash_available + item["capital"] + item["realized_pnl"], 2)

        entered_today = [item for item in normalized_trades if item["entry_date"] == day_str]
        for item in entered_today:
            cash_available = round(cash_available - item["capital"], 2)

        active = [
            item for item in normalized_trades
            if item["entry_date"] <= day_str < item["exit_date"]
        ]
        realized_today = round(
            sum(item["realized_pnl"] for item in normalized_trades if item["exit_date"] == day_str),
            2,
        )
        cumulative_realized_pnl = round(cumulative_realized_pnl + realized_today, 2)
        capital_deployed = round(sum(item["capital"] for item in active), 2)
        exposure_pct = round(capital_deployed / normalized_capital, 4)
        peak_exposure = max(peak_exposure, exposure_pct)
        max_positions = max(max_positions, len(active))
        max_capital_deployed = max(max_capital_deployed, capital_deployed)
        min_cash_available = min(min_cash_available, cash_available)
        timeline.append({
            "date": day_str,
            "open_position_count": len(active),
            "entry_count": len(entered_today),
            "exit_count": len(exited_today),
            "entries": [
                {
                    "code": item.get("code", ""),
                    "capital": item.get("capital", 0.0),
                    "priority_score": item.get("priority_score", 0.0),
                    "allocation_rank": item.get("allocation_rank", 0),
                }
                for item in entered_today
            ],
            "exits": [
                {
                    "code": item.get("code", ""),
                    "capital_released": item.get("capital", 0.0),
                    "realized_pnl": item.get("realized_pnl", 0.0),
                }
                for item in exited_today
            ],
            "capital_deployed": capital_deployed,
            "exposure_pct": exposure_pct,
            "cash_available": round(cash_available, 2),
            "realized_pnl_today": realized_today,
            "cumulative_realized_pnl": cumulative_realized_pnl,
        })

    return {
        "summary": {
            "capital": round(normalized_capital, 2),
            "total_exposure_max": round(total_exposure_max, 4),
            "single_position_max": round(single_position_max, 4),
            "total_cap_limit": total_cap_limit,
            "single_cap_limit": single_cap_limit,
            "timeline_days": len(timeline),
            "max_concurrent_positions": max_positions,
            "peak_exposure_pct": round(peak_exposure, 4),
            "max_capital_deployed": round(max_capital_deployed, 2),
            "ending_realized_pnl": round(cumulative_realized_pnl, 2),
            "ending_cash": round(cash_available, 2),
            "min_cash_available": round(min_cash_available, 2),
            "accepted_trade_count": len(normalized_trades),
            "constrained_trade_count": constrained_trade_count,
            "rejected_trade_count": rejected_trade_count,
            "allocation_rule": "entry_score_desc_realized_pnl_desc_capital_asc",
            "simulation_mode": "daily_event_replay",
            "intraday_ordering": "exits_before_entries",
        },
        "accepted_trades": accepted_trade_details,
        "rejected_trades": rejected_trade_details,
        "timeline": timeline,
    }


def _format_walk_forward_parameters(parameters: dict[str, Any]) -> str:
    if not parameters:
        return ""
    ordered_keys = [
        ("buy_threshold", "buy"),
        ("stop_loss", "stop"),
        ("take_profit", "take"),
        ("technical_weight", "tech"),
        ("fundamental_weight", "fund"),
        ("flow_weight", "flow"),
        ("sentiment_weight", "sent"),
    ]
    parts = []
    for source_key, label in ordered_keys:
        value = parameters.get(source_key)
        if value in (None, ""):
            continue
        parts.append(f"{label}={value}")
    if not parts:
        for key in sorted(parameters):
            value = parameters.get(key)
            if value in (None, ""):
                continue
            parts.append(f"{key}={value}")
    return ", ".join(parts)


def _markdown_table(headers: list[str], rows: list[list[Any]]) -> str:
    if not headers:
        return ""
    lines = [
        "| " + " | ".join(str(header) for header in headers) + " |",
        "|" + "|".join([" --- "] * len(headers)) + "|",
    ]
    for row in rows:
        lines.append("| " + " | ".join(str(cell) for cell in row) + " |")
    return "\n".join(lines)


def _build_walk_forward_comparison_summary(fold_reports: list[dict[str, Any]]) -> dict[str, Any]:
    if not fold_reports:
        return {
            "fold_count": 0,
            "training_total_realized_pnl": 0.0,
            "evaluation_total_realized_pnl": 0.0,
            "training_mean_win_rate": 0.0,
            "evaluation_mean_win_rate": 0.0,
            "training_mean_sample_count": 0.0,
            "evaluation_mean_sample_count": 0.0,
            "train_eval_pnl_delta": 0.0,
            "train_eval_win_rate_delta": 0.0,
            "train_eval_sample_count_delta": 0.0,
            "best_eval_fold": {},
            "best_train_fold": {},
            "rows": [],
        }

    rows = []
    training_total_realized_pnl = 0.0
    evaluation_total_realized_pnl = 0.0
    training_total_win_rate = 0.0
    evaluation_total_win_rate = 0.0
    training_total_sample_count = 0
    evaluation_total_sample_count = 0
    best_eval_fold = None
    best_train_fold = None
    for item in fold_reports:
        training_summary = item["score_summary"]["training_summary"]
        evaluation_summary = item["score_summary"]["evaluation_summary"]
        selected_parameters = item["score_summary"].get("selected_parameters", {})
        training_pnl = round(_safe_float(training_summary.get("total_realized_pnl", 0.0), 0.0), 2)
        evaluation_pnl = round(_safe_float(evaluation_summary.get("total_realized_pnl", 0.0), 0.0), 2)
        training_win_rate = round(_safe_float(training_summary.get("win_rate", 0.0), 0.0), 1)
        evaluation_win_rate = round(_safe_float(evaluation_summary.get("win_rate", 0.0), 0.0), 1)
        training_sample_count = int(training_summary.get("closed_trade_count", 0) or 0)
        evaluation_sample_count = int(evaluation_summary.get("closed_trade_count", 0) or 0)
        row = {
            "fold": item.get("fold", 0),
            "train_window": f"{item.get('train_start', '')} -> {item.get('train_end', '')}",
            "test_window": f"{item.get('test_start', '')} -> {item.get('test_end', '')}",
            "selected_parameters": _format_walk_forward_parameters(selected_parameters),
            "training_pnl": training_pnl,
            "training_win_rate": training_win_rate,
            "training_sample_count": training_sample_count,
            "evaluation_pnl": evaluation_pnl,
            "evaluation_win_rate": evaluation_win_rate,
            "evaluation_sample_count": evaluation_sample_count,
            "train_eval_pnl_delta": round(evaluation_pnl - training_pnl, 2),
            "train_eval_win_rate_delta": round(evaluation_win_rate - training_win_rate, 1),
            "train_eval_sample_count_delta": evaluation_sample_count - training_sample_count,
        }
        rows.append(row)
        training_total_realized_pnl += training_pnl
        evaluation_total_realized_pnl += evaluation_pnl
        training_total_win_rate += training_win_rate
        evaluation_total_win_rate += evaluation_win_rate
        training_total_sample_count += training_sample_count
        evaluation_total_sample_count += evaluation_sample_count
        if best_eval_fold is None or evaluation_pnl > _safe_float(best_eval_fold["evaluation_pnl"], 0.0):
            best_eval_fold = row
        if best_train_fold is None or training_pnl > _safe_float(best_train_fold["training_pnl"], 0.0):
            best_train_fold = row

    fold_count = len(rows)
    return {
        "fold_count": fold_count,
        "training_total_realized_pnl": round(training_total_realized_pnl, 2),
        "evaluation_total_realized_pnl": round(evaluation_total_realized_pnl, 2),
        "training_mean_win_rate": round(training_total_win_rate / fold_count, 1),
        "evaluation_mean_win_rate": round(evaluation_total_win_rate / fold_count, 1),
        "training_mean_sample_count": round(training_total_sample_count / fold_count, 1),
        "evaluation_mean_sample_count": round(evaluation_total_sample_count / fold_count, 1),
        "train_eval_pnl_delta": round(evaluation_total_realized_pnl - training_total_realized_pnl, 2),
        "train_eval_win_rate_delta": round((evaluation_total_win_rate - training_total_win_rate) / fold_count, 1),
        "train_eval_sample_count_delta": round((evaluation_total_sample_count - training_total_sample_count) / fold_count, 1),
        "best_eval_fold": best_eval_fold or {},
        "best_train_fold": best_train_fold or {},
        "rows": rows,
    }


def _score_parameter_set(summary: dict[str, Any]) -> tuple[float, float, int]:
    return (
        _safe_float(summary.get("total_realized_pnl", 0.0), 0.0),
        _safe_float(summary.get("win_rate", 0.0), 0.0),
        int(summary.get("closed_trade_count", 0) or 0),
    )


def _select_best_parameter_set(
    trades: list[dict[str, Any]],
    params_grid: list[dict[str, float]],
    baseline: dict[str, float],
) -> tuple[dict[str, float], list[dict[str, Any]], dict[str, Any]]:
    rankings = []
    for params in params_grid:
        evaluated = _apply_parameter_set(trades, params, baseline)
        summary = _summarize_trade_list(evaluated)
        rankings.append({
            "params": params,
            "summary": summary,
            "objective": {
                "total_realized_pnl": summary["total_realized_pnl"],
                "win_rate": summary["win_rate"],
                "sample_count": summary["closed_trade_count"],
            },
        })
    rankings.sort(key=lambda item: _score_parameter_set(item["summary"]), reverse=True)
    best = rankings[0] if rankings else {"params": baseline, "summary": _summarize_trade_list(trades), "objective": {}}
    return best["params"], rankings, best["summary"]


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
    buy_thresholds: str | list[float] | None = None,
    stop_losses: str | list[float] | None = None,
    take_profits: str | list[float] | None = None,
    technical_weights: str | list[float] | None = None,
    fundamental_weights: str | list[float] | None = None,
    flow_weights: str | list[float] | None = None,
    sentiment_weights: str | list[float] | None = None,
) -> dict[str, Any]:
    inputs = load_backtest_inputs(start, end, scope=scope, fixture=fixture)
    start_date = _parse_date(inputs["start"])
    end_date = _parse_date(inputs["end"])
    baseline = _baseline_parameters()
    params_grid = _parameter_grid(
        buy_thresholds=buy_thresholds,
        stop_losses=stop_losses,
        take_profits=take_profits,
        technical_weights=technical_weights,
        fundamental_weights=fundamental_weights,
        flow_weights=flow_weights,
        sentiment_weights=sentiment_weights,
    )
    source_trades = _filter_closed_trades(inputs.get("trade_review", {}), start_date, end_date)
    selected_params, rankings, selected_summary = _select_best_parameter_set(source_trades, params_grid, baseline)
    sample_payload = _build_history_samples(
        source_trades,
        scope=inputs["scope"],
        start=inputs["start"],
        end=inputs["end"],
        source_mode=inputs["source_mode"],
    )
    excursion_status = str((inputs.get("trade_review", {}) or {}).get("mfe_mae_status", "proxy_market_history"))
    sample_store = {
        "sample_count": sample_payload["sample_count"],
        "path": _persist_history_samples(sample_payload) if sample_payload["sample_count"] else "",
        "source": excursion_status,
    }
    evaluated_trades = _apply_parameter_set(source_trades, selected_params, baseline)
    portfolio_replay = _build_portfolio_replay(
        evaluated_trades,
        start=start_date,
        end=end_date,
        total_capital=_safe_float(get_strategy().get("capital", 450286), 450286),
        strategy=get_strategy(),
    )
    score_summary = _summarize_score(inputs)
    risk_summary = _summarize_risk(inputs)
    score_summary.update({
        "selected_summary": selected_summary,
        "baseline_parameters": baseline,
        "selected_parameters": selected_params,
        "grid_size": len(params_grid),
    })
    sample_count = int(selected_summary["closed_trade_count"])
    status = _status_from_components(
        str(inputs.get("state_audit", {}).get("status", "ok")),
        str(score_summary.get("market_signal", "ok")),
        str(risk_summary.get("risk_state", "ok")),
    )
    if sample_count == 0:
        status = "warning" if status == "ok" else status
    result = {
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
            "engine_mode": "proxy_parameter_sweep",
        },
        "sample_count": sample_count,
        "score_summary": score_summary,
        "risk_summary": risk_summary,
        "state_fields": _state_fields(inputs),
        "selected_parameters": selected_params,
        "parameter_rankings": rankings[:10],
        "sample_store": sample_store,
        "portfolio_replay": portfolio_replay,
    }
    result_path, report_path = _persist_backtest_outputs("run", result)
    result["result_path"] = result_path
    result["report_path"] = report_path
    result["artifacts"] = _build_artifacts(result_path, report_path, sample_store.get("path", ""))
    _update_backtest_index("run", result, result_path, report_path, sample_store.get("path", ""))
    return result


def run_parameter_sweep(
    start: str,
    end: str,
    *,
    scope: str = "cn_a_system",
    fixture: str | Path | None = None,
    buy_thresholds: str | list[float] | None = None,
    stop_losses: str | list[float] | None = None,
    take_profits: str | list[float] | None = None,
    technical_weights: str | list[float] | None = None,
    fundamental_weights: str | list[float] | None = None,
    flow_weights: str | list[float] | None = None,
    sentiment_weights: str | list[float] | None = None,
) -> dict[str, Any]:
    inputs = load_backtest_inputs(start, end, scope=scope, fixture=fixture)
    start_date = _parse_date(inputs["start"])
    end_date = _parse_date(inputs["end"])
    baseline = _baseline_parameters()
    params_grid = _parameter_grid(
        buy_thresholds=buy_thresholds,
        stop_losses=stop_losses,
        take_profits=take_profits,
        technical_weights=technical_weights,
        fundamental_weights=fundamental_weights,
        flow_weights=flow_weights,
        sentiment_weights=sentiment_weights,
    )
    trades = _filter_closed_trades(inputs.get("trade_review", {}), start_date, end_date)
    selected_params, rankings, selected_summary = _select_best_parameter_set(trades, params_grid, baseline)
    sample_payload = _build_history_samples(
        trades,
        scope=inputs["scope"],
        start=inputs["start"],
        end=inputs["end"],
        source_mode=inputs["source_mode"],
    )
    excursion_status = str((inputs.get("trade_review", {}) or {}).get("mfe_mae_status", "proxy_market_history"))
    replay_trades = _apply_parameter_set(trades, selected_params, baseline)
    result = {
        "command": "backtest",
        "action": "sweep",
        "status": "ok" if rankings else "warning",
        "parameters": {
            "start": inputs["start"],
            "end": inputs["end"],
            "scope": inputs["scope"],
            "window_days": inputs["window_days"],
            "fixture": inputs.get("fixture_path", ""),
            "source_mode": inputs["source_mode"],
            "engine_mode": "proxy_parameter_sweep",
        },
        "sample_count": int(selected_summary["closed_trade_count"]),
        "baseline_parameters": baseline,
        "selected_parameters": selected_params,
        "ranking_count": len(rankings),
        "rankings": rankings[:20],
        "portfolio_replay": _build_portfolio_replay(
            replay_trades,
            start=start_date,
            end=end_date,
            total_capital=_safe_float(get_strategy().get("capital", 450286), 450286),
            strategy=get_strategy(),
        ),
        "sample_store": {
            "sample_count": sample_payload["sample_count"],
            "path": _persist_history_samples(sample_payload) if sample_payload["sample_count"] else "",
            "source": excursion_status,
        },
    }
    result_path, report_path = _persist_backtest_outputs("sweep", result)
    result["result_path"] = result_path
    result["report_path"] = report_path
    result["artifacts"] = _build_artifacts(result_path, report_path, result["sample_store"].get("path", ""))
    _update_backtest_index("sweep", result, result_path, report_path, result["sample_store"].get("path", ""))
    return result


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
    buy_thresholds: str | list[float] | None = None,
    stop_losses: str | list[float] | None = None,
    take_profits: str | list[float] | None = None,
    technical_weights: str | list[float] | None = None,
    fundamental_weights: str | list[float] | None = None,
    flow_weights: str | list[float] | None = None,
    sentiment_weights: str | list[float] | None = None,
) -> dict[str, Any]:
    start_date = _parse_date(start)
    end_date = _parse_date(end)
    if end_date < start_date:
        raise ValueError("end must be on or after start")

    inputs = load_backtest_inputs(start, end, scope=scope, fixture=fixture)
    all_trades = _filter_closed_trades(inputs.get("trade_review", {}), start_date, end_date)
    baseline = _baseline_parameters()
    params_grid = _parameter_grid(
        buy_thresholds=buy_thresholds,
        stop_losses=stop_losses,
        take_profits=take_profits,
        technical_weights=technical_weights,
        fundamental_weights=fundamental_weights,
        flow_weights=flow_weights,
        sentiment_weights=sentiment_weights,
    )
    windows = _walk_forward_windows(start_date, end_date, folds)
    fold_reports = []
    for index, window in enumerate(windows, start=1):
        train_start = _parse_date(window["train_start"])
        train_end = _parse_date(window["train_end"])
        test_start = _parse_date(window["test_start"])
        test_end = _parse_date(window["test_end"])
        train_trades = [item for item in all_trades if _date_in_range(str(item.get("exit_date", "")), train_start, train_end)]
        test_trades = [item for item in all_trades if _date_in_range(str(item.get("exit_date", "")), test_start, test_end)]
        selected_params, rankings, training_summary = _select_best_parameter_set(train_trades, params_grid, baseline)
        evaluated_test = _apply_parameter_set(test_trades, selected_params, baseline)
        evaluation_summary = _summarize_trade_list(evaluated_test)
        fold_score = {
            "training_summary": training_summary,
            "evaluation_summary": evaluation_summary,
            "selected_parameters": selected_params,
            "ranking_count": len(rankings),
        }
        fold_risk = _summarize_risk(inputs)
        fold_reports.append({
            "fold": index,
            "train_start": window["train_start"],
            "train_end": window["train_end"],
            "test_start": window["test_start"],
            "test_end": window["test_end"],
            "training_sample_count": int(training_summary["closed_trade_count"]),
            "sample_count": int(evaluation_summary["closed_trade_count"]),
            "score_summary": fold_score,
            "risk_summary": fold_risk,
            "status": _status_from_components(
                str(fold_risk.get("risk_state", "ok")),
                "warning" if int(evaluation_summary["closed_trade_count"]) == 0 else "ok",
            ),
        })

    total_sample_count = sum(item["sample_count"] for item in fold_reports)
    sample_payload = _build_history_samples(
        all_trades,
        scope=inputs["scope"],
        start=inputs["start"],
        end=inputs["end"],
        source_mode=inputs["source_mode"],
    )
    excursion_status = str((inputs.get("trade_review", {}) or {}).get("mfe_mae_status", "proxy_market_history"))
    score_summary = {
        "fold_count": len(fold_reports),
        "mean_win_rate": round(
            sum(item["score_summary"]["evaluation_summary"]["win_rate"] for item in fold_reports) / len(fold_reports), 1
        ) if fold_reports else 0.0,
        "total_realized_pnl": round(
            sum(item["score_summary"]["evaluation_summary"]["total_realized_pnl"] for item in fold_reports), 2
        ),
        "mean_sample_count": round(total_sample_count / len(fold_reports), 1) if fold_reports else 0.0,
        "baseline_parameters": baseline,
        "grid_size": len(params_grid),
    }
    risk_summary = {
        "fold_count": len(fold_reports),
        "worst_risk_state": max((item["risk_summary"]["risk_state"] for item in fold_reports), key=_severity_rank, default="ok"),
        "worst_pool_sync_state": max((item["risk_summary"]["pool_sync_state"] for item in fold_reports), key=_severity_rank, default="ok"),
    }
    replay_trades = _apply_parameter_set(all_trades, baseline, baseline)
    overall_status = max((item["status"] for item in fold_reports), key=_severity_rank, default="ok")
    if total_sample_count == 0 and overall_status == "ok":
        overall_status = "warning"
    comparison_summary = _build_walk_forward_comparison_summary(fold_reports)
    result = {
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
            "engine_mode": "proxy_walk_forward",
        },
        "sample_count": total_sample_count,
        "score_summary": score_summary,
        "risk_summary": risk_summary,
        "comparison_summary": comparison_summary,
        "state_fields": _state_fields(inputs),
        "folds": fold_reports,
        "portfolio_replay": _build_portfolio_replay(
            replay_trades,
            start=start_date,
            end=end_date,
            total_capital=_safe_float(get_strategy().get("capital", 450286), 450286),
            strategy=get_strategy(),
        ),
        "sample_store": {
            "sample_count": sample_payload["sample_count"],
            "path": _persist_history_samples(sample_payload) if sample_payload["sample_count"] else "",
            "source": excursion_status,
        },
    }
    result_path, report_path = _persist_backtest_outputs("walk_forward", result)
    result["result_path"] = result_path
    result["report_path"] = report_path
    result["artifacts"] = _build_artifacts(result_path, report_path, result["sample_store"].get("path", ""))
    _update_backtest_index("walk_forward", result, result_path, report_path, result["sample_store"].get("path", ""))
    return result
