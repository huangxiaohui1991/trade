"""
backtest/strategy_replay.py — 真正的逐日策略回放引擎

与 _build_portfolio_replay（基于闭合交易样本的回放）不同，
本模块实现信号驱动的逐日策略模拟：

  每个交易日：
    1. 读取大盘信号 → 决定是否允许开仓
    2. 遍历候选池，逐票评分 → 应用 veto → 生成买入信号
    3. 按优先级排序，在资金/仓位约束下分配资金
    4. 管理已持仓位：止损 / 止盈 / 时间止损 / 大盘清仓
    5. 记录当日组合快照

输入：
  - daily_data: dict[date_str, DaySnapshot]
    每日快照包含 market_signal, candidates (带评分/veto), prices
  - strategy params (可被 sweep 覆盖)

输出：
  与 _build_portfolio_replay 兼容的 summary / timeline / trades 结构
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _parse_date(value: str) -> date:
    from datetime import datetime
    return datetime.strptime(str(value).strip(), "%Y-%m-%d").date()


def _iter_dates(start: date, end: date) -> list[date]:
    items: list[date] = []
    current = start
    while current <= end:
        items.append(current)
        current += timedelta(days=1)
    return items


# ---------------------------------------------------------------------------
# 持仓管理
# ---------------------------------------------------------------------------

class _Position:
    """单个持仓的运行时状态。"""

    __slots__ = (
        "code", "name", "entry_date", "entry_price", "shares", "capital",
        "stop_price", "take_price", "time_stop_date", "entry_score",
        "veto_warnings",
    )

    def __init__(
        self,
        code: str,
        name: str,
        entry_date: str,
        entry_price: float,
        shares: int,
        capital: float,
        stop_price: float,
        take_price: float,
        time_stop_date: str,
        entry_score: float,
        veto_warnings: list[str] | None = None,
    ):
        self.code = code
        self.name = name
        self.entry_date = entry_date
        self.entry_price = entry_price
        self.shares = shares
        self.capital = capital
        self.stop_price = stop_price
        self.take_price = take_price
        self.time_stop_date = time_stop_date
        self.entry_score = entry_score
        self.veto_warnings = veto_warnings or []


# ---------------------------------------------------------------------------
# 核心引擎
# ---------------------------------------------------------------------------

def run_strategy_replay(
    daily_data: dict[str, dict[str, Any]],
    *,
    start: str,
    end: str,
    total_capital: float = 450286,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    信号驱动的逐日策略回放。

    Args:
        daily_data: 每日快照，key 为 "YYYY-MM-DD"，value 包含：
            - market_signal: "GREEN" | "YELLOW" | "RED" | "CLEAR"
            - candidates: list[dict] 每个候选包含：
                code, name, score (总分), veto_signals, price,
                technical_score, fundamental_score, flow_score, sentiment_score
            - prices: dict[code, float] 当日收盘价（用于持仓盯市）
        start / end: 回放区间
        total_capital: 初始资金
        params: 策略参数覆盖，支持：
            buy_threshold, watch_threshold, reject_threshold,
            stop_loss, take_profit, time_stop_days,
            total_max, single_max, weekly_max,
            veto_rules: list[str] 启用的 veto 规则名
            consecutive_loss_days_limit, cooldown_days,
            technical_weight, fundamental_weight, flow_weight, sentiment_weight

    Returns:
        与 _build_portfolio_replay 兼容的结构
    """
    params = params or {}
    start_date = _parse_date(start)
    end_date = _parse_date(end)

    # ── 策略参数 ──
    buy_threshold = _safe_float(params.get("buy_threshold", 7), 7)
    stop_loss_pct = _safe_float(params.get("stop_loss", 0.04), 0.04)
    take_profit_pct = _safe_float(params.get("take_profit", 0.15), 0.15)
    time_stop_days = int(params.get("time_stop_days", 15) or 15)
    total_max = max(min(_safe_float(params.get("total_max", 0.60), 0.60), 1.0), 0.0)
    single_max = max(min(_safe_float(params.get("single_max", 0.20), 0.20), 1.0), 0.0)
    weekly_max = int(params.get("weekly_max", 2) or 2)
    veto_rules = set(params.get("veto_rules", [
        "below_ma20", "limit_up_today", "consecutive_outflow", "red_market", "earnings_bomb",
    ]))
    consecutive_loss_days_limit = int(params.get("consecutive_loss_days_limit", 0) or 0)
    cooldown_days_cfg = int(params.get("cooldown_days", 0) or 0)

    # 权重（用于重算总分）
    weights = {
        "technical": _safe_float(params.get("technical_weight", 2), 2),
        "fundamental": _safe_float(params.get("fundamental_weight", 3), 3),
        "flow": _safe_float(params.get("flow_weight", 2), 2),
        "sentiment": _safe_float(params.get("sentiment_weight", 3), 3),
    }

    normalized_capital = max(total_capital, 1.0)
    total_cap_limit = round(normalized_capital * total_max, 2)
    single_cap_limit = round(normalized_capital * single_max, 2)

    # ── 运行时状态 ──
    cash = round(normalized_capital, 2)
    positions: list[_Position] = []
    timeline: list[dict[str, Any]] = []
    closed_trades: list[dict[str, Any]] = []
    rejected_entries: list[dict[str, Any]] = []

    peak_exposure = 0.0
    max_positions = 0
    max_capital_deployed = 0.0
    min_cash = round(normalized_capital, 2)
    cumulative_pnl = 0.0
    constrained_count = 0
    rejected_count = 0
    cooldown_rejected_count = 0
    veto_rejected_count = 0

    # 冷却状态
    consecutive_loss_days = 0
    cooldown_until = ""

    # 周买入计数
    weekly_buy_counts: dict[str, int] = {}  # iso_week -> count

    for day in _iter_dates(start_date, end_date):
        day_str = day.isoformat()
        iso_week = day.isocalendar()[1]
        week_key = f"{day.isocalendar()[0]}-W{iso_week:02d}"
        snapshot = daily_data.get(day_str, {})
        market_signal = str(snapshot.get("market_signal", "GREEN")).upper()
        candidates = snapshot.get("candidates", [])
        prices = snapshot.get("prices", {})

        entries_today: list[dict[str, Any]] = []
        exits_today: list[dict[str, Any]] = []

        # ── 1. 管理已有持仓：检查止损/止盈/时间止损/大盘清仓 ──
        surviving: list[_Position] = []
        for pos in positions:
            current_price = _safe_float(prices.get(pos.code, 0), 0)
            exit_reason = ""

            if market_signal in ("RED", "CLEAR"):
                exit_reason = "market_signal_exit"
                exit_price = current_price if current_price > 0 else pos.entry_price
            elif current_price > 0 and current_price <= pos.stop_price:
                exit_reason = "stop_loss"
                exit_price = pos.stop_price
            elif current_price > 0 and current_price >= pos.take_price:
                exit_reason = "take_profit"
                exit_price = pos.take_price
            elif pos.time_stop_date and day_str >= pos.time_stop_date:
                exit_reason = "time_stop"
                exit_price = current_price if current_price > 0 else pos.entry_price
            else:
                surviving.append(pos)
                continue

            # 平仓
            realized_pnl = round((exit_price - pos.entry_price) * pos.shares, 2)
            cash = round(cash + pos.capital + realized_pnl, 2)
            cumulative_pnl = round(cumulative_pnl + realized_pnl, 2)
            trade_record = {
                "code": pos.code,
                "name": pos.name,
                "entry_date": pos.entry_date,
                "exit_date": day_str,
                "entry_price": pos.entry_price,
                "exit_price": round(exit_price, 4),
                "shares": pos.shares,
                "capital": pos.capital,
                "realized_pnl": realized_pnl,
                "exit_reason": exit_reason,
                "entry_score": pos.entry_score,
                "holding_days": (_parse_date(day_str) - _parse_date(pos.entry_date)).days,
            }
            closed_trades.append(trade_record)
            exits_today.append({
                "code": pos.code,
                "capital_released": pos.capital,
                "realized_pnl": realized_pnl,
                "exit_reason": exit_reason,
            })

        positions = surviving

        # ── 更新冷却状态 ──
        if exits_today:
            day_exit_pnl = sum(e["realized_pnl"] for e in exits_today)
            if day_exit_pnl < 0:
                consecutive_loss_days += 1
            elif day_exit_pnl > 0:
                consecutive_loss_days = 0
            if (consecutive_loss_days_limit > 0 and cooldown_days_cfg > 0
                    and consecutive_loss_days >= consecutive_loss_days_limit):
                cooldown_until = (day + timedelta(days=cooldown_days_cfg)).isoformat()

        # ── 2. 评估新入场信号 ──
        if market_signal not in ("RED", "CLEAR"):
            # 冷却检查
            in_cooldown = bool(cooldown_until and day_str <= cooldown_until)

            # 已持有的代码
            held_codes = {pos.code for pos in positions}

            # 对候选重算评分并过滤
            scored_candidates: list[dict[str, Any]] = []
            for cand in candidates:
                code = str(cand.get("code", "")).strip()
                if not code or code in held_codes:
                    continue

                # 重算加权总分
                score = _recompute_score(cand, weights)
                if score is None:
                    score = _safe_float(cand.get("score", 0), 0)

                if score < buy_threshold:
                    continue

                # veto 检查
                cand_veto = cand.get("veto_signals", []) or []
                hard_veto = [v for v in cand_veto if v in veto_rules and v != "consecutive_outflow_warn"]
                if hard_veto:
                    veto_rejected_count += 1
                    rejected_entries.append({
                        "code": code,
                        "date": day_str,
                        "score": round(score, 2),
                        "reason": f"veto:{','.join(hard_veto)}",
                    })
                    continue

                price = _safe_float(cand.get("price", 0), 0)
                if price <= 0:
                    continue

                scored_candidates.append({
                    "code": code,
                    "name": str(cand.get("name", code)),
                    "score": round(score, 2),
                    "price": price,
                    "veto_warnings": [v for v in cand_veto if v == "consecutive_outflow_warn"],
                })

            # 按评分降序排列
            scored_candidates.sort(key=lambda c: (-c["score"], c["code"]))

            # 分配资金
            for cand in scored_candidates:
                if in_cooldown:
                    cooldown_rejected_count += 1
                    rejected_count += 1
                    rejected_entries.append({
                        "code": cand["code"],
                        "date": day_str,
                        "score": cand["score"],
                        "reason": "portfolio_cooldown",
                    })
                    continue

                # 周买入次数限制
                week_buys = weekly_buy_counts.get(week_key, 0)
                if week_buys >= weekly_max:
                    rejected_count += 1
                    rejected_entries.append({
                        "code": cand["code"],
                        "date": day_str,
                        "score": cand["score"],
                        "reason": "weekly_max_reached",
                    })
                    continue

                current_deployed = round(sum(p.capital for p in positions), 2)
                remaining_total = max(total_cap_limit - current_deployed, 0.0)
                desired_capital = round(cand["price"] * _estimate_shares(cand["price"], single_cap_limit), 2)
                capital = min(desired_capital, single_cap_limit, remaining_total, cash)

                if capital <= 0:
                    rejected_count += 1
                    rejected_entries.append({
                        "code": cand["code"],
                        "date": day_str,
                        "score": cand["score"],
                        "reason": "capital_limit",
                    })
                    continue

                if capital < desired_capital:
                    constrained_count += 1

                shares = max(int(capital / cand["price"] // 100) * 100, 100)
                actual_capital = round(cand["price"] * shares, 2)
                if actual_capital > capital:
                    shares = max(shares - 100, 100)
                    actual_capital = round(cand["price"] * shares, 2)
                if actual_capital <= 0 or actual_capital > cash:
                    continue

                # 计算止损/止盈/时间止损价
                stop_price = round(cand["price"] * (1 - stop_loss_pct), 4)
                take_price = round(cand["price"] * (1 + take_profit_pct), 4)
                time_stop_dt = (day + timedelta(days=time_stop_days)).isoformat() if time_stop_days > 0 else ""

                pos = _Position(
                    code=cand["code"],
                    name=cand["name"],
                    entry_date=day_str,
                    entry_price=cand["price"],
                    shares=shares,
                    capital=actual_capital,
                    stop_price=stop_price,
                    take_price=take_price,
                    time_stop_date=time_stop_dt,
                    entry_score=cand["score"],
                    veto_warnings=cand.get("veto_warnings", []),
                )
                positions.append(pos)
                cash = round(cash - actual_capital, 2)
                weekly_buy_counts[week_key] = week_buys + 1

                entries_today.append({
                    "code": cand["code"],
                    "capital": actual_capital,
                    "score": cand["score"],
                    "shares": shares,
                })

        # ── 3. 当日快照 ──
        capital_deployed = round(sum(p.capital for p in positions), 2)
        exposure_pct = round(capital_deployed / normalized_capital, 4)
        peak_exposure = max(peak_exposure, exposure_pct)
        max_positions = max(max_positions, len(positions))
        max_capital_deployed = max(max_capital_deployed, capital_deployed)
        min_cash = min(min_cash, cash)

        realized_today = round(sum(e["realized_pnl"] for e in exits_today), 2)

        timeline.append({
            "date": day_str,
            "market_signal": market_signal,
            "open_position_count": len(positions),
            "entry_count": len(entries_today),
            "exit_count": len(exits_today),
            "entries": entries_today,
            "exits": exits_today,
            "capital_deployed": capital_deployed,
            "exposure_pct": exposure_pct,
            "cash_available": round(cash, 2),
            "realized_pnl_today": realized_today,
            "cumulative_realized_pnl": round(cumulative_pnl, 2),
        })

    # ── 未平仓标记 ──
    open_positions = [
        {
            "code": p.code,
            "name": p.name,
            "entry_date": p.entry_date,
            "entry_price": p.entry_price,
            "shares": p.shares,
            "capital": p.capital,
            "entry_score": p.entry_score,
            "holding_days": (end_date - _parse_date(p.entry_date)).days,
        }
        for p in positions
    ]

    # ── 汇总 ──
    win_trades = [t for t in closed_trades if t["realized_pnl"] > 0]
    loss_trades = [t for t in closed_trades if t["realized_pnl"] < 0]

    return {
        "summary": {
            "capital": round(normalized_capital, 2),
            "total_exposure_max": round(total_max, 4),
            "single_position_max": round(single_max, 4),
            "total_cap_limit": total_cap_limit,
            "single_cap_limit": single_cap_limit,
            "timeline_days": len(timeline),
            "max_concurrent_positions": max_positions,
            "peak_exposure_pct": round(peak_exposure, 4),
            "max_capital_deployed": round(max_capital_deployed, 2),
            "ending_realized_pnl": round(cumulative_pnl, 2),
            "ending_cash": round(cash, 2),
            "min_cash_available": round(min_cash, 2),
            "closed_trade_count": len(closed_trades),
            "win_count": len(win_trades),
            "loss_count": len(loss_trades),
            "win_rate": round(len(win_trades) / len(closed_trades) * 100, 1) if closed_trades else 0.0,
            "total_realized_pnl": round(cumulative_pnl, 2),
            "open_position_count": len(open_positions),
            "constrained_trade_count": constrained_count,
            "rejected_trade_count": rejected_count,
            "cooldown_rejected_count": cooldown_rejected_count,
            "veto_rejected_count": veto_rejected_count,
            "consecutive_loss_days_limit": consecutive_loss_days_limit,
            "cooldown_days": cooldown_days_cfg,
            "allocation_rule": "score_desc",
            "simulation_mode": "signal_driven_strategy_replay",
            "intraday_ordering": "exits_before_entries",
        },
        "closed_trades": closed_trades,
        "open_positions": open_positions,
        "rejected_entries": rejected_entries[:100],
        "timeline": timeline,
    }


# ---------------------------------------------------------------------------
# 辅助
# ---------------------------------------------------------------------------

def _recompute_score(candidate: dict[str, Any], weights: dict[str, float]) -> float | None:
    """用自定义权重重算四维总分。"""
    components = [
        ("technical_score", "technical", 2.0),
        ("fundamental_score", "fundamental", 3.0),
        ("flow_score", "flow", 2.0),
        ("sentiment_score", "sentiment", 3.0),
    ]
    if not any(candidate.get(k) not in (None, "") for k, _, _ in components):
        return None
    total = 0.0
    for score_key, weight_key, denom in components:
        val = candidate.get(score_key)
        if val in (None, ""):
            continue
        total += _safe_float(val, 0.0) * _safe_float(weights.get(weight_key, denom), denom) / denom
    return round(total, 2)


def _estimate_shares(price: float, max_capital: float) -> int:
    """估算可买股数（A 股 100 股整手）。"""
    if price <= 0:
        return 0
    raw = max_capital / price
    return max(int(raw // 100) * 100, 100)
