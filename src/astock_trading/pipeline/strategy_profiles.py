"""P6-2 多策略 profile 对比。

只做配置和历史证据对比；不自动切换 ASTOCK_CONFIG_PROFILE。
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from statistics import mean
from typing import Any

from astock_trading.platform.config import ConfigRegistry
from astock_trading.platform.domain_events import (
    DECISION_SUGGESTED,
    STRATEGY_CAPITAL_ALLOCATION_PROPOSED,
    STRATEGY_PROFILE_COMPARISON_PROPOSED,
    TRADE_REVIEW_RECORDED,
)
from astock_trading.platform.events import EventStore
from astock_trading.platform.paths import resolve_config_dir
from astock_trading.platform.time import utc_now_iso

DEFAULT_PROFILES = ("trend_swing", "short_continuation", "defensive_watch")
ACTIVE_STRATEGY_BUDGET_PCT = 0.60


def compare_strategy_profiles(
    conn: Any,
    *,
    config_dir: Path | None = None,
    profiles: tuple[str, ...] = DEFAULT_PROFILES,
    record: bool = False,
) -> dict:
    """比较多个策略 profile 的配置差异和已有运行证据。"""
    config_root = config_dir or resolve_config_dir()
    store = EventStore(conn)
    rows = [
        _profile_summary(conn, store, config_root=config_root, profile=name)
        for name in profiles
    ]
    has_evidence = any(item["evidence_status"] == "has_profile_runs" for item in rows)
    payload = {
        "analysis": "strategy_profile_comparison",
        "status": "ok" if has_evidence else "needs_shadow_validation",
        "generated_at": utc_now_iso(),
        "current_profile": os.getenv("ASTOCK_CONFIG_PROFILE", "default"),
        "profiles": rows,
        "recommendations": _recommendations(rows),
        "guardrails": {
            "auto_switch_profile": False,
            "auto_allocate_capital": False,
            "manual_approval_required": True,
            "reason": "P6-2 只做多策略 profile 对比，不自动切换 ASTOCK_CONFIG_PROFILE，也不自动分配资金。",
        },
        "recorded_event_id": "",
    }
    payload["report_markdown"] = render_strategy_profile_report(payload)

    if record:
        event_id = store.append(
            "strategy:profiles",
            "strategy",
            STRATEGY_PROFILE_COMPARISON_PROPOSED,
            payload={key: value for key, value in payload.items() if key != "recorded_event_id"},
            metadata={"source": "strategy_profiles"},
        )
        payload["recorded_event_id"] = event_id
        _write_report_artifact(conn, event_id, payload["report_markdown"])

    return payload


def propose_strategy_allocation(
    conn: Any,
    *,
    config_dir: Path | None = None,
    profiles: tuple[str, ...] = DEFAULT_PROFILES,
    total_capital: float = 500000.0,
    min_samples: int = 10,
    record: bool = False,
) -> dict:
    """生成多策略隔离资金桶和弱策略处理建议；不自动执行。"""
    comparison = compare_strategy_profiles(conn, config_dir=config_dir, profiles=profiles, record=False)
    total_capital_cents = int(round(max(total_capital, 0.0) * 100))
    buckets = _capital_buckets(
        comparison.get("profiles") or [],
        total_capital_cents=total_capital_cents,
        min_samples=min_samples,
    )
    weak_review = _weak_strategy_review(buckets, min_samples=min_samples)
    payload = {
        "analysis": "strategy_capital_allocation",
        "status": _allocation_status(buckets),
        "generated_at": utc_now_iso(),
        "current_profile": comparison.get("current_profile", "default"),
        "total_capital_cents": total_capital_cents,
        "capital_policy": {
            "mode": "advisory_only",
            "active_strategy_budget_pct": ACTIVE_STRATEGY_BUDGET_PCT,
            "reserve_pct": round(1 - ACTIVE_STRATEGY_BUDGET_PCT, 4),
            "min_review_samples": min_samples,
        },
        "capital_buckets": buckets,
        "weak_strategy_review": weak_review,
        "source_profile_comparison": {
            "status": comparison.get("status"),
            "profile_count": len(comparison.get("profiles") or []),
            "recommendations": comparison.get("recommendations", []),
        },
        "recommendations": _allocation_recommendations(buckets),
        "guardrails": {
            "auto_apply": False,
            "auto_switch_profile": False,
            "auto_allocate_capital": False,
            "manual_approval_required": True,
            "reason": "只输出隔离资金桶和弱策略处理建议，不改账户、不切换 profile、不自动分配资金。",
        },
        "recorded_event_id": "",
    }
    payload["report_markdown"] = render_strategy_allocation_report(payload)

    if record:
        store = EventStore(conn)
        event_id = store.append(
            "strategy:allocation",
            "strategy",
            STRATEGY_CAPITAL_ALLOCATION_PROPOSED,
            payload={key: value for key, value in payload.items() if key != "recorded_event_id"},
            metadata={"source": "strategy_allocation"},
        )
        payload["recorded_event_id"] = event_id
        _write_report_artifact(
            conn,
            event_id,
            payload["report_markdown"],
            report_type="strategy_capital_allocation",
            artifact_prefix="strategy_allocation",
        )

    return payload


def profile_config_hash(config: dict) -> str:
    """返回与 ConfigRegistry.freeze() 一致的配置 hash 前缀。"""
    config_json = json.dumps(config, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(config_json.encode()).hexdigest()[:16]


def render_strategy_profile_report(payload: dict) -> str:
    """渲染中文多策略 profile 对比报告。"""
    status_label = {
        "ok": "已有运行证据",
        "needs_shadow_validation": "需要影子验证",
    }.get(str(payload.get("status") or ""), str(payload.get("status") or ""))
    lines = [
        "# P6-2 多策略 Profile 对比",
        "",
        f"- 状态：{status_label}",
        f"- 当前执行 profile：{payload.get('current_profile')}",
        "- 自动切换 profile：否",
        "- 自动资金分配：否",
        "",
        "## Profile 概览",
    ]
    for item in payload.get("profiles") or []:
        review = item.get("trade_review") or {}
        params = item.get("key_parameters") or {}
        lines.extend([
            f"- {item.get('name')}：{item.get('purpose')}",
            f"  - 买入阈值：{params.get('buy_threshold')}；观察阈值：{params.get('watch_threshold')}",
            f"  - 历史运行：{item.get('run_count')} 次；复盘样本：{review.get('sample_count')} 笔",
            f"  - 平均收益：{review.get('avg_return_pct', 0):.2%}；胜率：{review.get('win_rate_pct', 0):.2%}",
        ])
    lines.extend(["", "## 建议"])
    for recommendation in payload.get("recommendations") or []:
        lines.append(f"- {recommendation}")
    return "\n".join(lines)


def render_strategy_allocation_report(payload: dict) -> str:
    """渲染中文多策略隔离资金建议报告。"""
    status_label = {
        "ok": "可人工复核",
        "review_required": "需要人工复核",
        "needs_shadow_validation": "需要影子验证",
    }.get(str(payload.get("status") or ""), str(payload.get("status") or ""))
    lines = [
        "# P6-2 多策略隔离资金建议",
        "",
        f"- 状态：{status_label}",
        f"- 总资金：¥{(payload.get('total_capital_cents', 0) or 0) / 100:,.2f}",
        "- 自动分配资金：否",
        "- 自动切换 profile：否",
        "",
        "## 隔离资金桶",
    ]
    for bucket in payload.get("capital_buckets") or []:
        lines.extend([
            f"- {bucket.get('profile')}（{bucket.get('scope')}）：{bucket.get('display_action')}",
            f"  - 建议资金：¥{bucket.get('suggested_capital_cents', 0) / 100:,.2f}"
            f"（{bucket.get('suggested_capital_pct', 0):.1%}）",
            f"  - 依据：{bucket.get('reason')}",
        ])
    lines.extend(["", "## 弱策略复核"])
    review = payload.get("weak_strategy_review") or {}
    lines.append(f"- 暂停候选：{', '.join(review.get('pause_candidates') or []) or '无'}")
    lines.append(f"- 影子验证：{', '.join(review.get('shadow_candidates') or []) or '无'}")
    lines.extend(["", "## 建议"])
    for recommendation in payload.get("recommendations") or []:
        lines.append(f"- {recommendation}")
    return "\n".join(lines)


def _profile_summary(conn: Any, store: EventStore, *, config_root: Path, profile: str) -> dict:
    config, errors = ConfigRegistry(config_dir=config_root, profile=profile).load_and_validate()
    strategy = config.get("strategy", {})
    config_hash = profile_config_hash(config)
    versions = _matching_config_versions(conn, config_hash)
    run_count = _run_count(conn, versions)
    decisions = _decision_counts(store, versions)
    trade_review = _trade_review_stats(store, versions)
    evidence_status = "has_profile_runs" if run_count or sum(decisions.values()) or trade_review["sample_count"] else "no_profile_runs"
    return {
        "name": profile,
        "purpose": _profile_purpose(profile),
        "config_hash": config_hash,
        "matched_config_versions": versions,
        "config_errors": errors,
        "evidence_status": evidence_status,
        "run_count": run_count,
        "decision_counts": decisions,
        "trade_review": trade_review,
        "key_parameters": _key_parameters(strategy),
    }


def _capital_buckets(profiles: list[dict], *, total_capital_cents: int, min_samples: int) -> list[dict]:
    active_profiles = [item for item in profiles if _allocation_action(item, min_samples) == "activate_candidate"]
    active_scores = {
        item["name"]: max(item["trade_review"]["avg_return_pct"], 0.001)
        * max(item["trade_review"]["win_rate_pct"], 0.001)
        for item in active_profiles
    }
    score_sum = sum(active_scores.values())
    active_budget_cents = int(round(total_capital_cents * ACTIVE_STRATEGY_BUDGET_PCT))

    buckets = []
    for item in profiles:
        action = _allocation_action(item, min_samples)
        if action == "activate_candidate" and score_sum > 0:
            capital_cents = int(round(active_budget_cents * active_scores[item["name"]] / score_sum))
        else:
            capital_cents = 0
        buckets.append(_capital_bucket(item, action=action, capital_cents=capital_cents, total_cents=total_capital_cents))
    return buckets


def _capital_bucket(profile: dict, *, action: str, capital_cents: int, total_cents: int) -> dict:
    review = profile.get("trade_review") or {}
    params = profile.get("key_parameters") or {}
    return {
        "profile": profile.get("name"),
        "scope": f"strategy_{_scope_slug(str(profile.get('name') or 'unknown'))}",
        "action": action,
        "display_action": _allocation_action_label(action),
        "suggested_capital_cents": capital_cents,
        "suggested_capital_pct": round(capital_cents / total_cents, 4) if total_cents > 0 else 0.0,
        "max_single_position_pct": params.get("single_max_pct", 0.0),
        "review_sample_count": review.get("sample_count", 0),
        "avg_return_pct": review.get("avg_return_pct", 0.0),
        "win_rate_pct": review.get("win_rate_pct", 0.0),
        "reason": _allocation_reason(profile, action),
    }


def _allocation_action(profile: dict, min_samples: int) -> str:
    review = profile.get("trade_review") or {}
    sample_count = int(review.get("sample_count") or 0)
    avg_return = float(review.get("avg_return_pct") or 0.0)
    win_rate = float(review.get("win_rate_pct") or 0.0)
    if sample_count < min_samples:
        return "shadow_validate"
    if avg_return < 0 or win_rate < 0.4:
        return "pause_candidate"
    return "activate_candidate"


def _allocation_action_label(action: str) -> str:
    return {
        "activate_candidate": "可作为人工复核后的启用候选",
        "pause_candidate": "建议暂停并列入弱策略复核",
        "shadow_validate": "仅影子验证，暂不分配执行资金",
    }.get(action, action)


def _allocation_reason(profile: dict, action: str) -> str:
    review = profile.get("trade_review") or {}
    samples = int(review.get("sample_count") or 0)
    avg_return = float(review.get("avg_return_pct") or 0.0)
    win_rate = float(review.get("win_rate_pct") or 0.0)
    if action == "activate_candidate":
        return f"已有 {samples} 笔复盘样本，平均收益 {avg_return:.2%}，胜率 {win_rate:.2%}。"
    if action == "pause_candidate":
        return f"已有 {samples} 笔复盘样本，但平均收益 {avg_return:.2%}、胜率 {win_rate:.2%} 未达标。"
    return f"复盘样本只有 {samples} 笔，先积累影子运行证据。"


def _weak_strategy_review(buckets: list[dict], *, min_samples: int) -> dict:
    return {
        "rules": {
            "min_review_samples": min_samples,
            "pause_when_avg_return_below": 0,
            "pause_when_win_rate_below": 0.4,
        },
        "active_candidates": [item["profile"] for item in buckets if item["action"] == "activate_candidate"],
        "pause_candidates": [item["profile"] for item in buckets if item["action"] == "pause_candidate"],
        "shadow_candidates": [item["profile"] for item in buckets if item["action"] == "shadow_validate"],
    }


def _allocation_status(buckets: list[dict]) -> str:
    if not buckets or all(item["action"] == "shadow_validate" for item in buckets):
        return "needs_shadow_validation"
    if any(item["action"] == "pause_candidate" for item in buckets):
        return "review_required"
    return "ok"


def _allocation_recommendations(buckets: list[dict]) -> list[str]:
    if not buckets:
        return ["没有可分配的策略 profile。"]
    recommendations = [
        "把每个 profile 的建议资金桶当作人工审批清单；当前实现不会自动改账户或真实资金。",
    ]
    if any(item["action"] == "shadow_validate" for item in buckets):
        recommendations.append("证据不足的 profile 只做影子运行，先补 run_log、decision.suggested 和 trade.review.recorded。")
    if any(item["action"] == "pause_candidate" for item in buckets):
        recommendations.append("暂停候选 profile 需要复核样本来源，确认不是行情阶段或数据质量造成的短期偏差。")
    return recommendations


def _matching_config_versions(conn: Any, config_hash: str) -> list[str]:
    rows = conn.execute(
        """SELECT config_version
           FROM config_versions
           WHERE config_hash = ?
           ORDER BY created_at DESC""",
        (config_hash,),
    ).fetchall()
    return [str(row["config_version"]) for row in rows]


def _run_count(conn: Any, config_versions: list[str]) -> int:
    if not config_versions:
        return 0
    placeholders = ", ".join("?" for _ in config_versions)
    row = conn.execute(
        f"""SELECT COUNT(*) AS count
            FROM run_log
            WHERE config_version IN ({placeholders}) AND status = 'completed'""",
        tuple(config_versions),
    ).fetchone()
    return int(row["count"] or 0) if row else 0


def _decision_counts(store: EventStore, config_versions: list[str]) -> dict[str, int]:
    counts = {"BUY": 0, "WATCH": 0, "CLEAR": 0, "SELL": 0, "NO_TRADE": 0}
    for version in config_versions:
        for event in store.query(event_type=DECISION_SUGGESTED, metadata_filter={"config_version": version}, limit=5000):
            action = str((event.get("payload") or {}).get("action") or "NO_TRADE")
            counts[action] = counts.get(action, 0) + 1
    return counts


def _trade_review_stats(store: EventStore, config_versions: list[str]) -> dict:
    returns = []
    for version in config_versions:
        events = store.query(event_type=TRADE_REVIEW_RECORDED, metadata_filter={"config_version": version}, limit=5000)
        for event in events:
            returns.append(_float((event.get("payload") or {}).get("latest_return_pct")))
    return {
        "sample_count": len(returns),
        "avg_return_pct": round(mean(returns), 4) if returns else 0.0,
        "win_rate_pct": round(sum(1 for value in returns if value > 0) / len(returns), 4) if returns else 0.0,
    }


def _key_parameters(strategy: dict) -> dict:
    scoring = strategy.get("scoring", {})
    thresholds = scoring.get("thresholds", {})
    gates = scoring.get("decision_gates", {})
    position = strategy.get("risk", {}).get("position", {})
    auto_trade = strategy.get("auto_trade", {})
    continuation = strategy.get("continuation", {})
    continuation_scoring = continuation.get("scoring", {})
    return {
        "buy_threshold": _float(thresholds.get("buy")),
        "watch_threshold": _float(thresholds.get("watch")),
        "reject_threshold": _float(thresholds.get("reject")),
        "require_entry_signal_for_buy": bool(gates.get("require_entry_signal_for_buy", False)),
        "min_data_quality_for_buy": str(gates.get("min_data_quality_for_buy", "degraded")),
        "max_missing_fields_for_buy": gates.get("max_missing_fields_for_buy"),
        "single_max_pct": _float(position.get("single_max")),
        "total_max_pct": _float(position.get("total_max")),
        "weekly_max": int(position.get("weekly_max", 0) or 0),
        "continuation_top_n": int(continuation_scoring.get("top_n", 0) or 0),
        "continuation_hold_days": continuation_scoring.get("hold_days", []),
        "auto_trade_enabled": bool(auto_trade.get("enabled", False)),
        "auto_trade_dry_run": bool(auto_trade.get("dry_run", True)),
    }


def _profile_purpose(profile: str) -> str:
    return {
        "trend_swing": "趋势波段候选，适合 5-20 个交易日的确认型机会。",
        "short_continuation": "短线续涨研究，适合 T+1 到 T+3 的强势延续样本验证。",
        "defensive_watch": "弱市观察模式，提高买入门槛，优先减少新开仓。",
    }.get(profile, "自定义策略 profile。")


def _recommendations(rows: list[dict]) -> list[str]:
    if not rows:
        return ["没有发现可比较的策略 profile。"]
    if not any(row["evidence_status"] == "has_profile_runs" for row in rows):
        return [
            "先做影子运行并积累每个 profile 的 run_log、decision.suggested 和 trade.review.recorded，再比较胜率与收益。",
            "在有足够样本前，不要自动切换 ASTOCK_CONFIG_PROFILE，也不要做自动资金隔离。",
        ]
    ranked = sorted(
        rows,
        key=lambda item: (
            item["trade_review"]["sample_count"],
            item["trade_review"]["avg_return_pct"],
            item["run_count"],
        ),
        reverse=True,
    )
    top = ranked[0]
    return [
        f"当前证据最多的是 {top['name']}，但仍需结合样本数量、市场状态和人工复核决定是否用于执行。",
        "profile 对比只产生建议；执行前必须显式确认 ASTOCK_CONFIG_PROFILE。",
    ]


def _write_report_artifact(
    conn: Any,
    event_id: str,
    markdown: str,
    *,
    report_type: str = "strategy_profile_comparison",
    artifact_prefix: str = "strategy_profiles",
) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO report_artifacts
           (artifact_id, run_id, report_type, format, content, delivered_to, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            f"{artifact_prefix}_{event_id}",
            event_id,
            report_type,
            "markdown",
            markdown,
            "local",
            utc_now_iso(),
        ),
    )


def _float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _scope_slug(value: str) -> str:
    return "".join(char if char.isalnum() else "_" for char in value).strip("_") or "unknown"
