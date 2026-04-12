#!/usr/bin/env python3
"""
pipeline/shadow_trade.py — 影子交易引擎

用妙想模拟盘自动执行交易系统的买卖信号，验证策略有效性。

功能：
  - buy_new_picks: 核心池新入池股票 → 模拟盘市价买入
  - check_stop_signals: 检查模拟盘持仓的止损/止盈信号 → 自动卖出
  - get_performance: 拉模拟盘数据，统计胜率/盈亏比
  - sync_report: 生成模拟盘周报写入 Obsidian

用法：
  python -m scripts.pipeline.shadow_trade buy     # 对核心池执行买入
  python -m scripts.pipeline.shadow_trade check   # 检查止损止盈
  python -m scripts.pipeline.shadow_trade status   # 查看模拟盘状态
  python -m scripts.pipeline.shadow_trade report   # 生成报告
"""

import os
import sys
import math
from datetime import date, datetime
from pathlib import Path

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from scripts.mx.cli_tools import MXCommandError, dispatch_mx_command, list_mx_command_metadata, mx_command_groups
from scripts.engine.scorer import (
    data_quality_blocks_auto_buy,
    data_quality_review_reason,
    normalize_data_quality,
    score as score_stock,
)
from scripts.state import load_activity_summary, load_order_snapshot, record_trade_event, upsert_order_state
from scripts.utils.config_loader import get_stocks, get_strategy
from scripts.utils.logger import get_logger
from scripts.utils.obsidian import ObsidianVault

_logger = get_logger("pipeline.shadow_trade")

# 每只股票的模拟买入金额（元）
POSITION_SIZE = 20000
AUTOMATED_RISK_RULES = {
    "RISK_DYNAMIC_STOP": "动态止损",
    "RISK_ABSOLUTE_STOP": "绝对止损",
    "RISK_TAKE_PROFIT_T1": "第一批止盈",
}
ADVISORY_RISK_RULES = {
    "RISK_TIME_STOP": "时间止损",
    "RISK_DRAWDOWN_TAKE_PROFIT": "回撤止盈",
}
AUTOMATION_SCOPE_NOTE = (
    "本波仅自动执行：动态止损、绝对止损、第一批止盈；"
    "时间止损与回撤止盈仅作为提示，不自动下单。"
)
ORDER_REASON_TO_CONDITION = {
    "RISK_DYNAMIC_STOP": "dynamic_stop",
    "RISK_ABSOLUTE_STOP": "absolute_stop",
    "RISK_TAKE_PROFIT_T1": "take_profit_t1",
    "BUY_CORE_POOL": "core_pool_entry",
}


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------

def _mx_health_snapshot(include_unavailable: bool = False) -> dict:
    items = list_mx_command_metadata(include_unavailable=include_unavailable)
    groups = mx_command_groups(include_unavailable=include_unavailable)
    unavailable_items = [item for item in items if not item.get("available", False)]
    available_items = [item for item in items if item.get("available", False)]
    required_commands = [
        "mx.data.query",
        "mx.moni.positions",
        "mx.moni.balance",
        "mx.moni.orders",
        "mx.moni.buy",
        "mx.moni.sell",
        "mx.moni.cancel",
        "mx.moni.cancel_all",
        "mx.zixuan.query",
        "mx.zixuan.manage",
    ]
    item_lookup = {item.get("id", ""): item for item in items}
    required = {
        command_id: {
            "available": bool(item_lookup.get(command_id, {}).get("available", False)),
            "availability_note": item_lookup.get(command_id, {}).get("availability_note", ""),
        }
        for command_id in required_commands
    }
    status = "ok" if not unavailable_items else "warning"
    return {
        "status": status,
        "available_count": len(available_items),
        "unavailable_count": len(unavailable_items),
        "command_count": len(items),
        "group_count": len(groups),
        "groups": {name: len(values) for name, values in groups.items()},
        "required": required,
        "unavailable_commands": [item.get("id", "") for item in unavailable_items],
        "source": "scripts.mx.cli_tools",
    }


def _mx_dispatch(command: str, **kwargs) -> dict:
    try:
        result = dispatch_mx_command(command, **kwargs)
        return result if isinstance(result, dict) else {"data": result}
    except MXCommandError as exc:
        _logger.warning(f"[mx] {command} 调用失败: {exc}")
        raise


def _query_mx(command: str, *, fallback: list | dict | None = None, **kwargs):
    try:
        return _mx_dispatch(command, **kwargs)
    except MXCommandError:
        return fallback if fallback is not None else {}


def _log_trade(action: str, code: str, name: str, shares: int,
               price: float, reason: str = "", reason_code: str = "",
               source: str = "shadow_trade", metadata: dict | None = None) -> None:
    """
    记录模拟盘交易到 Obsidian 交易日志。
    追加到 02-运行/模拟盘/交易记录.md
    """
    vault = ObsidianVault()
    log_dir = Path(vault.vault_path) / vault.paper_trade_dir
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "交易记录.md"
    log_relative = f"{vault.paper_trade_dir}/交易记录.md"

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    amount = round(shares * price, 2)

    # 如果文件不存在，写表头（通过 vault.write 触发自动备份）
    if not log_path.exists():
        header = (
            "# 模拟盘交易记录\n\n"
            "| 时间 | 操作 | 股票 | 代码 | 数量 | 价格 | 金额 | 原因 |\n"
            "|------|------|------|------|------|------|------|------|\n"
        )
        vault.write(log_relative, header)

    # 追加一行
    reason_text = f"[{reason_code}] {reason}".strip() if reason_code else reason
    line = f"| {now} | {action} | {name} | {code} | {shares} | ¥{price:.2f} | ¥{amount:,.0f} | {reason_text} |\n"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(line)

    try:
        side = "buy" if "买" in action else "sell" if "卖" in action else action.lower()
        record_trade_event({
            "external_id": f"shadow:{datetime.now().strftime('%Y%m%d%H%M%S%f')}:{code}:{side}:{shares}",
            "scope": "paper_mx",
            "market": "MX_PAPER",
            "code": code,
            "name": name,
            "side": side,
            "event_type": side,
            "shares": shares,
            "price": price,
            "amount": amount,
            "event_date": datetime.now().strftime("%Y-%m-%d"),
            "reason_code": reason_code or f"paper_{side}",
            "reason_text": reason,
            "source": source,
            "metadata": {
                "action": action,
                "log_path": str(log_path),
                **(metadata or {}),
            },
        })
    except Exception as exc:
        _logger.warning(f"[shadow] 结构化事件写入失败: {exc}")

    _logger.info(f"[shadow] 交易记录: {action} {name}({code}) {shares}股 @ ¥{price:.2f}")


def _trade_result(code: str, name: str, shares: int, status: str,
                  reason: str = "", reason_code: str = "",
                  **extra) -> dict:
    payload = {
        "code": code,
        "name": name,
        "shares": shares,
        "status": status,
        "reason": reason,
        "reason_code": reason_code,
    }
    payload.update(extra)
    return payload


def _pick_first(payload: dict, keys: list[str], default=""):
    for key in keys:
        value = payload.get(key)
        if value not in [None, ""]:
            return value
    return default


def _normalize_order_side(value) -> str:
    text = str(value or "").strip().lower()
    if text in {"buy", "b", "1"} or "买" in text:
        return "buy"
    if text in {"sell", "s", "2"} or "卖" in text:
        return "sell"
    return text


def _normalize_order_status(value) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return "placed"
    if any(token in text for token in ["cancel replace", "replace", "改单", "改挂", "撤改单"]):
        return "cancel_replace_pending"
    if any(token in text for token in ["cancel pending", "cancel_requested", "撤单中", "待撤", "已报待撤"]):
        return "cancel_requested"
    if any(token in text for token in ["part", "partial", "部分成交", "部成"]):
        return "partially_filled"
    if any(token in text for token in ["filled", "全部成交", "已成交", "成交", "success", "done"]):
        return "filled"
    if any(token in text for token in ["cancel", "撤单", "已撤", "撤销"]):
        return "cancelled"
    if any(token in text for token in ["reject", "error", "fail", "exception", "废单", "失败"]):
        return "exception"
    if any(token in text for token in ["review pending", "review", "复核", "待复核"]):
        return "review_required"
    if any(token in text for token in ["candidate", "候选"]):
        return "candidate"
    return "placed"


def _normalize_order_type(value, use_market_price: bool | None = None) -> str:
    text = str(value or "").strip().lower()
    if use_market_price is True:
        return "market"
    if any(token in text for token in ["market", "市价"]):
        return "market"
    if any(token in text for token in ["condition", "conditional", "条件"]):
        return "conditional"
    if any(token in text for token in ["limit", "限价"]):
        return "limit"
    return "market" if use_market_price else "limit"


def _extract_broker_order_id(payload: dict) -> str:
    return str(
        _pick_first(
            payload,
            [
                "orderId",
                "order_id",
                "brokerOrderId",
                "broker_order_id",
                "entrustNo",
                "orderNo",
                "tradeId",
                "id",
            ],
            "",
        )
    ).strip()


def _existing_order_maps(scope: str = "paper_mx") -> tuple[dict[str, dict], dict[str, dict]]:
    snapshot = load_order_snapshot(scope=scope)
    by_external = {}
    by_broker = {}
    for order in snapshot.get("orders", []):
        external_id = str(order.get("external_id", "")).strip()
        broker_order_id = str(order.get("broker_order_id", "")).strip()
        if external_id:
            by_external[external_id] = order
        if broker_order_id:
            by_broker[broker_order_id] = order
    return by_external, by_broker


def _sync_broker_orders(_client: object = None, scope: str = "paper_mx") -> dict:
    try:
        if _client is not None and hasattr(_client, "orders"):
            raw_orders = _get_orders(_client)
        else:
            raw_orders = _get_orders()
    except Exception as exc:
        _logger.warning(f"[shadow] 拉取委托失败: {exc}")
        return {"status": "error", "error": str(exc), "fetched_count": 0, "synced_count": 0}

    _, by_broker = _existing_order_maps(scope=scope)
    synced_orders = []
    for index, raw_order in enumerate(raw_orders):
        broker_order_id = _extract_broker_order_id(raw_order)
        existing = by_broker.get(broker_order_id, {})
        external_id = (
            existing.get("external_id")
            or (f"paper_mx:order:{broker_order_id}" if broker_order_id else "")
            or f"paper_mx:order:sync:{index}:{_pick_first(raw_order, ['stockCode', 'secuCode', 'code'], '')}"
        )
        side = _normalize_order_side(_pick_first(raw_order, ["type", "side", "orderSide", "bsFlag"], ""))
        status = _normalize_order_status(
            _pick_first(raw_order, ["status", "orderStatus", "statusDesc", "orderStatusDesc"], "")
        )
        raw_status = str(_pick_first(raw_order, ["status", "orderStatus", "statusDesc", "orderStatusDesc"], "")).strip()
        use_market_price = bool(_pick_first(raw_order, ["useMarketPrice", "marketPriceFlag"], False))
        filled_shares = _safe_int(
            _pick_first(
                raw_order,
                ["filledQuantity", "filledQty", "dealQty", "成交数量", "dealQuantity"],
                0,
            ),
            0,
        )
        requested_shares = _safe_int(
            _pick_first(raw_order, ["quantity", "qty", "orderQty", "entrustQty", "委托数量"], filled_shares),
            filled_shares,
        )
        avg_fill_price = _safe_float(
            _pick_first(raw_order, ["avgPrice", "avgFillPrice", "dealPrice", "成交均价"], 0.0),
            0.0,
        )
        trigger_price = _safe_float(
            _pick_first(raw_order, ["triggerPrice", "stopPrice", "conditionPrice"], 0.0),
            0.0,
        )
        limit_price = _safe_float(
            _pick_first(raw_order, ["price", "orderPrice", "entrustPrice", "委托价格"], 0.0),
            0.0,
        )
        payload = upsert_order_state(
            {
                "external_id": external_id,
                "scope": scope,
                "broker": "mx_moni",
                "broker_order_id": broker_order_id,
                "code": str(_pick_first(raw_order, ["stockCode", "secuCode", "code"], "")).strip(),
                "name": str(_pick_first(raw_order, ["stockName", "secuName", "name"], "")).strip(),
                "side": side,
                "order_class": existing.get("order_class", "condition"),
                "order_type": _normalize_order_type(
                    _pick_first(raw_order, ["orderType", "priceType", "typeDesc"], ""),
                    use_market_price=use_market_price,
                ),
                "condition_type": existing.get("condition_type", ""),
                "requested_shares": requested_shares,
                "filled_shares": filled_shares,
                "trigger_price": trigger_price,
                "limit_price": limit_price,
                "avg_fill_price": avg_fill_price,
                "status": status,
                "confirm_status": existing.get("confirm_status", "not_required"),
                "reason_code": existing.get("reason_code", ""),
                "reason_text": existing.get("reason_text", ""),
                "source": "mx_orders_sync",
                "placed_at": str(
                    _pick_first(raw_order, ["placedAt", "orderTime", "entrustTime", "createTime"], existing.get("placed_at", ""))
                ).strip(),
                "filled_at": str(
                    _pick_first(raw_order, ["filledAt", "dealTime", "成交时间"], existing.get("filled_at", ""))
                ).strip(),
                "cancelled_at": str(
                    _pick_first(raw_order, ["cancelledAt", "cancelTime", "撤单时间"], existing.get("cancelled_at", ""))
                ).strip(),
                "updated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "metadata": {
                    **(existing.get("metadata", {}) if isinstance(existing.get("metadata", {}), dict) else {}),
                    "raw_order": raw_order,
                    "broker_status_raw": raw_status,
                    "broker_status_normalized": status,
                    "broker_synced_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                },
            }
        )
        payload = _materialize_filled_order(payload)
        synced_orders.append(payload)

    return {
        "status": "ok",
        "fetched_count": len(raw_orders),
        "synced_count": len(synced_orders),
    }


def _materialize_filled_order(order: dict) -> dict:
    metadata = order.get("metadata", {}) if isinstance(order.get("metadata", {}), dict) else {}
    status = str(order.get("status", "")).strip()
    if status not in {"filled", "partially_filled"}:
        return order

    total_filled = _safe_int(order.get("filled_shares", order.get("requested_shares", 0)), 0)
    logged_filled = _safe_int(metadata.get("broker_logged_filled_shares", 0), 0)
    if metadata.get("trade_event_logged") and status == "filled":
        logged_filled = max(logged_filled, total_filled)
    shares = max(total_filled - logged_filled, 0)
    price = _safe_float(order.get("avg_fill_price", order.get("limit_price", 0.0)), 0.0)
    code = str(order.get("code", "")).strip()
    if not code or shares <= 0 or price <= 0:
        return order

    side = _normalize_order_side(order.get("side", ""))
    action_text = "买入" if side == "buy" else "卖出"
    _log_trade(
        action_text,
        code,
        order.get("name", code),
        shares,
        price,
        str(order.get("reason_text", "")).strip(),
        str(order.get("reason_code", "")).strip(),
        source="shadow_order_sync",
        metadata={
            "order_external_id": order.get("external_id", ""),
            "broker_fill_status": status,
            "broker_logged_filled_shares_before": logged_filled,
        },
    )
    return upsert_order_state(
        {
            "external_id": order.get("external_id", ""),
            "updated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "metadata": {
                "trade_event_logged": status == "filled",
                "trade_event_source": "shadow_order_sync",
                "broker_logged_filled_shares": total_filled,
            },
        }
    )


def _submit_shadow_order(
    _client: object = None,
    *,
    side: str,
    code: str,
    name: str,
    shares: int,
    reason: str,
    reason_code: str,
    price: float = 0.0,
    use_market_price: bool = True,
    order_class: str = "manual",
) -> tuple[dict, dict]:
    now_ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    external_id = f"paper_mx:order:{datetime.now().strftime('%Y%m%d%H%M%S%f')}:{code}:{side}:{shares}"
    upsert_order_state(
        {
            "external_id": external_id,
            "scope": "paper_mx",
            "broker": "mx_moni",
            "code": code,
            "name": name,
            "side": side,
            "order_class": order_class,
            "order_type": _normalize_order_type("", use_market_price=use_market_price),
            "condition_type": ORDER_REASON_TO_CONDITION.get(reason_code, ""),
            "requested_shares": shares,
            "filled_shares": 0,
            "limit_price": price if not use_market_price else 0.0,
            "avg_fill_price": 0.0,
            "status": "candidate",
            "confirm_status": "not_required",
            "reason_code": reason_code,
            "reason_text": reason,
            "source": "shadow_trade",
            "placed_at": now_ts,
            "updated_at": now_ts,
            "metadata": {"trade_event_logged": False},
        }
    )

    if _client is not None and hasattr(_client, "trade"):
        if use_market_price:
            trade_result = _client.trade(side, code, shares, use_market_price=True)
        else:
            trade_result = _client.trade(side, code, shares, price=price, use_market_price=False)
    else:
        if use_market_price:
            trade_result = _query_mx(f"mx.moni.{side}", stock_code=code, quantity=shares, use_market_price=True)
        else:
            trade_result = _query_mx(
                f"mx.moni.{side}",
                stock_code=code,
                quantity=shares,
                price=price,
                use_market_price=False,
            )

    broker_order_id = _extract_broker_order_id(trade_result)
    trade_code = str(trade_result.get("code", ""))
    is_market_filled = trade_code == "200" and use_market_price
    order_status = "exception"
    if trade_code == "200":
        order_status = "filled" if is_market_filled else "placed"

    order = upsert_order_state(
        {
            "external_id": external_id,
            "scope": "paper_mx",
            "broker": "mx_moni",
            "broker_order_id": broker_order_id,
            "status": order_status,
            "requested_shares": shares,
            "filled_shares": shares if is_market_filled else 0,
            "avg_fill_price": price if is_market_filled else 0.0,
            "limit_price": price if not use_market_price else 0.0,
            "reason_code": reason_code,
            "reason_text": reason,
            "filled_at": now_ts if is_market_filled else "",
            "updated_at": now_ts,
            "metadata": {
                "raw_trade_result": trade_result,
                "trade_event_logged": False,
            },
        }
    )

    if is_market_filled:
        _log_trade(
            "买入" if side == "buy" else "卖出",
            code,
            name,
            shares,
            price,
            reason,
            reason_code,
            metadata={"order_external_id": external_id},
        )
        order = upsert_order_state(
            {
                "external_id": external_id,
                "updated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "metadata": {
                    "trade_event_logged": True,
                    "trade_event_source": "shadow_trade",
                },
            }
        )

    return trade_result, order


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value in [None, ""]:
            return default
        if isinstance(value, str):
            value = value.replace("¥", "").replace("%", "").replace(",", "").strip()
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value, default: int = 0) -> int:
    try:
        if value in [None, ""]:
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _parse_date(value) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    if len(text) >= 10:
        text = text[:10]
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


def _sort_trade_event_key(event: dict) -> tuple:
    event_date = _parse_date(
        event.get("event_date")
        or event.get("trade_date")
        or event.get("date")
    ) or date.min
    created_at = str(event.get("created_at") or event.get("timestamp") or "").strip()
    return (event_date.isoformat(), created_at)


def _build_open_position_context(trade_events: list) -> dict:
    """
    从结构化交易事件推断当前未平仓仓位的开仓日期与首买价。

    规则：
      - 仓位未归零前，open_date 保持首笔开仓日
      - 仓位清零后，下一次买入重新开始新的持仓周期
    """
    contexts = {}
    for event in sorted(trade_events or [], key=_sort_trade_event_key):
        code = str(event.get("code", "")).strip()
        side = str(event.get("side", event.get("action", ""))).strip().lower()
        shares = _safe_int(event.get("shares", 0))
        price = _safe_float(event.get("price", 0))
        event_date = _parse_date(
            event.get("event_date")
            or event.get("trade_date")
            or event.get("date")
        )

        if not code or side not in {"buy", "sell"} or shares <= 0 or not event_date:
            continue

        ctx = contexts.setdefault(code, {
            "name": str(event.get("name", "")).strip(),
            "open_date": "",
            "first_buy_price": 0.0,
            "last_buy_price": 0.0,
            "avg_cost": 0.0,
            "cost_basis_total": 0.0,
            "net_shares": 0,
            "last_event_date": "",
        })
        if str(event.get("name", "")).strip():
            ctx["name"] = str(event.get("name", "")).strip()

        if side == "buy":
            if ctx["net_shares"] <= 0:
                ctx["open_date"] = event_date.isoformat()
                ctx["first_buy_price"] = round(price, 3)
                ctx["cost_basis_total"] = 0.0
            ctx["last_buy_price"] = round(price, 3)
            ctx["cost_basis_total"] += shares * price
            ctx["net_shares"] += shares
            ctx["avg_cost"] = round(
                (ctx["cost_basis_total"] / ctx["net_shares"]) if ctx["net_shares"] > 0 else 0.0,
                3,
            )
        else:
            held_shares = _safe_int(ctx.get("net_shares", 0), 0)
            avg_cost = (
                (_safe_float(ctx.get("cost_basis_total", 0.0), 0.0) / held_shares)
                if held_shares > 0 else 0.0
            )
            reduced_shares = min(shares, held_shares)
            ctx["cost_basis_total"] = max(ctx["cost_basis_total"] - (reduced_shares * avg_cost), 0.0)
            ctx["net_shares"] = max(ctx["net_shares"] - shares, 0)
            if ctx["net_shares"] == 0:
                ctx["open_date"] = ""
                ctx["first_buy_price"] = 0.0
                ctx["last_buy_price"] = 0.0
                ctx["avg_cost"] = 0.0
                ctx["cost_basis_total"] = 0.0
            else:
                ctx["avg_cost"] = round(ctx["cost_basis_total"] / ctx["net_shares"], 3)

        ctx["last_event_date"] = event_date.isoformat()

    return {
        code: ctx
        for code, ctx in contexts.items()
        if _safe_int(ctx.get("net_shares", 0)) > 0
    }


def _load_history_points_since(code: str, open_date: date | None, today: date | None = None) -> list:
    if not code or not open_date:
        return []

    today = today or datetime.now().date()
    lookback_days = max((today - open_date).days + 10, 60)

    try:
        from scripts.engine.technical import _get_hist_data

        hist = _get_hist_data(code, days=lookback_days)
    except Exception as exc:
        _logger.info(f"[shadow] {code} 历史行情拉取失败: {exc}")
        return []

    if hist is None or getattr(hist, "empty", True):
        return []

    date_col = "日期" if "日期" in hist.columns else "date" if "date" in hist.columns else None
    close_col = "收盘" if "收盘" in hist.columns else "close" if "close" in hist.columns else None
    if not date_col or not close_col:
        return []

    points = []
    for _, row in hist.iterrows():
        point_date = _parse_date(row.get(date_col))
        close = _safe_float(row.get(close_col), 0.0)
        if not point_date or point_date < open_date or close <= 0:
            continue
        points.append({
            "date": point_date.isoformat(),
            "close": round(close, 3),
        })
    return points


def _build_advisory_signals(position: dict, trade_context: dict | None = None,
                            history_points: list | None = None,
                            risk_cfg: dict | None = None,
                            today: date | None = None) -> dict:
    risk_cfg = risk_cfg or get_strategy().get("risk", {})
    today = today or datetime.now().date()

    cost = _safe_float(position.get("cost", 0))
    price = _safe_float(position.get("price", 0))
    trade_context = trade_context or {}
    open_date = _parse_date(trade_context.get("open_date"))
    first_buy_price = _safe_float(trade_context.get("first_buy_price", cost), cost)
    hold_days = (today - open_date).days if open_date else None
    pnl_pct = ((price / cost) - 1) if cost > 0 and price > 0 else 0.0

    take_profit_cfg = risk_cfg.get("take_profit", {})
    t1_pct = _safe_float(take_profit_cfg.get("t1_pct", 0.15), 0.15)
    t1_drawdown = _safe_float(take_profit_cfg.get("t1_drawdown", 0.05), 0.05)
    t2_drawdown = _safe_float(take_profit_cfg.get("t2_drawdown", 0.08), 0.08)
    time_stop_days = _safe_int(risk_cfg.get("time_stop_days", 15), 15)

    signals = []
    if open_date and hold_days is not None and hold_days >= time_stop_days > 0 and pnl_pct < 0.02:
        signals.append({
            "rule_code": "RISK_TIME_STOP",
            "rule_name": ADVISORY_RISK_RULES["RISK_TIME_STOP"],
            "severity": "warning",
            "message": (
                f"已持有{hold_days}日，当前涨幅{pnl_pct*100:+.1f}%低于+2.0%，"
                "触发时间止损提示"
            ),
        })

    peak_close = 0.0
    peak_date = ""
    drawdown_pct = 0.0
    peak_gain_pct = 0.0
    target_base = first_buy_price if first_buy_price > 0 else cost
    history_points = history_points or []
    if history_points:
        peak_point = max(history_points, key=lambda item: _safe_float(item.get("close", 0), 0.0))
        peak_close = round(_safe_float(peak_point.get("close", 0), 0.0), 3)
        peak_date = str(peak_point.get("date", "")).strip()
        if peak_close > 0 and price > 0:
            drawdown_pct = max((peak_close - price) / peak_close, 0.0)
        if target_base > 0 and peak_close > 0:
            peak_gain_pct = (peak_close / target_base) - 1

    t1_price = round(target_base * (1 + t1_pct), 2) if target_base > 0 else 0.0
    if peak_close >= t1_price > 0 and drawdown_pct >= t1_drawdown:
        threshold = t2_drawdown if drawdown_pct >= t2_drawdown else t1_drawdown
        stage = "清仓级别" if threshold == t2_drawdown else "减仓级别"
        signals.append({
            "rule_code": "RISK_DRAWDOWN_TAKE_PROFIT",
            "rule_name": ADVISORY_RISK_RULES["RISK_DRAWDOWN_TAKE_PROFIT"],
            "severity": "high" if threshold == t2_drawdown else "warning",
            "message": (
                f"自最高收盘¥{peak_close:.2f}({peak_date or '未知日期'})回撤"
                f"{drawdown_pct*100:.1f}%，达到{threshold*100:.0f}%{stage}提示"
            ),
            "peak_close": peak_close,
            "peak_date": peak_date,
            "drawdown_pct": round(drawdown_pct, 4),
        })

    return {
        "open_date": open_date.isoformat() if open_date else "",
        "hold_days": hold_days,
        "first_buy_price": round(first_buy_price, 3) if first_buy_price else 0.0,
        "peak_close": peak_close,
        "peak_date": peak_date,
        "peak_gain_pct": round(peak_gain_pct, 4),
        "drawdown_pct": round(drawdown_pct, 4),
        "signals": signals,
        "summary": "；".join(signal["message"] for signal in signals),
        "triggered": bool(signals),
    }


def _build_shadow_position_view(raw_position: dict, risk_cfg: dict,
                                trade_context_map: dict | None = None,
                                history_cache: dict | None = None,
                                today: date | None = None) -> dict:
    # MX API 用分单位（costPrice=19340→¥19.34，price=1930→¥19.30）
    _price_dec = int(raw_position.get("priceDec", 2))
    _cost_dec  = int(raw_position.get("costPriceDec", 3))
    _price_raw  = float(raw_position.get("price", 0))
    _cost_raw   = float(raw_position.get("costPrice", 0))
    price = _price_raw  / (10 ** _price_dec) if _price_dec > 0 else _price_raw
    cost  = _cost_raw   / (10 ** _cost_dec)  if _cost_dec  > 0 else _cost_raw

    code   = str(raw_position.get("secCode", raw_position.get("stockCode",
               raw_position.get("secuCode", raw_position.get("code", ""))))).strip()
    name   = str(raw_position.get("secName", raw_position.get("stockName",
               raw_position.get("secuName", raw_position.get("name", ""))))).strip()
    shares = _safe_int(raw_position.get("count",
               raw_position.get("totalQty", raw_position.get("currentQty",
               raw_position.get("shares", 0)))))
    market_value = _safe_float(raw_position.get("value", 0)) if raw_position.get("value") else shares * price
    pnl     = _safe_float(raw_position.get("dayProfit", raw_position.get("profit",
                   raw_position.get("floatProfit", (price - cost) * shares))))
    pnl_pct = float(raw_position.get("dayProfitPct", raw_position.get("profitPct",
                   (price / cost - 1) if cost > 0 else 0.0)))  # MX dayProfitPct 已是百分比值，无需再×100

    trade_context = (trade_context_map or {}).get(code, {})
    open_date = _parse_date(trade_context.get("open_date"))
    cache_key = (code, open_date.isoformat() if open_date else "")
    history_cache = history_cache if history_cache is not None else {}
    if cache_key not in history_cache:
        history_cache[cache_key] = _load_history_points_since(code, open_date, today=today)
    advisory = _build_advisory_signals(
        {"code": code, "name": name, "shares": shares, "cost": cost, "price": price},
        trade_context=trade_context,
        history_points=history_cache.get(cache_key, []),
        risk_cfg=risk_cfg,
        today=today,
    )

    return {
        "code": code,
        "name": name,
        "shares": shares,
        "cost": cost,
        "price": price,
        "market_value": market_value,
        "pnl": pnl,
        "pnl_pct": pnl_pct,
        "open_date": advisory.get("open_date", ""),
        "hold_days": advisory.get("hold_days"),
        "first_buy_price": advisory.get("first_buy_price", 0.0),
        "peak_close": advisory.get("peak_close", 0.0),
        "peak_date": advisory.get("peak_date", ""),
        "drawdown_pct": advisory.get("drawdown_pct", 0.0),
        "advisory_signals": advisory.get("signals", []),
        "advisory_summary": advisory.get("summary", ""),
        "advisory_triggered": advisory.get("triggered", False),
    }


def _load_paper_position_context(window: int = 180) -> dict:
    try:
        summary = load_activity_summary(window, scope="paper_mx")
    except Exception as exc:
        _logger.info(f"[shadow] 结构化模拟盘事件读取失败: {exc}")
        return {}
    return _build_open_position_context(summary.get("trade_events", []))


def _actual_paper_position_map(positions: list) -> dict:
    mapped = {}
    for item in positions or []:
        code = str(item.get("code", "")).strip()
        shares = _safe_int(item.get("shares", 0))
        if not code or shares <= 0:
            continue
        mapped[code] = {
            "code": code,
            "name": str(item.get("name", "")).strip(),
            "shares": shares,
            "cost": _safe_float(item.get("cost", 0.0), 0.0),
            "price": _safe_float(item.get("price", 0.0), 0.0),
        }
    return mapped


def paper_trade_consistency_snapshot(window: int = 180) -> dict:
    activity = load_activity_summary(window, scope="paper_mx")
    inferred_context = _build_open_position_context(activity.get("trade_events", []))
    inferred_positions = {
        code: {
            "code": code,
            "name": str(item.get("name", "")).strip(),
            "shares": _safe_int(item.get("net_shares", 0)),
            "open_date": str(item.get("open_date", "")).strip(),
            "first_buy_price": _safe_float(item.get("first_buy_price", 0.0), 0.0),
            "avg_cost": _safe_float(item.get("avg_cost", 0.0), 0.0),
        }
        for code, item in inferred_context.items()
        if _safe_int(item.get("net_shares", 0)) > 0
    }

    shadow_status = get_status()
    actual_positions = _actual_paper_position_map(shadow_status.get("positions", []))

    inferred_codes = sorted(inferred_positions.keys())
    actual_codes = sorted(actual_positions.keys())
    event_only_codes = sorted(code for code in inferred_codes if code not in actual_codes)
    broker_only_codes = sorted(code for code in actual_codes if code not in inferred_codes)
    share_mismatches = []
    for code in sorted(set(inferred_codes) & set(actual_codes)):
        inferred_shares = inferred_positions[code]["shares"]
        actual_shares = actual_positions[code]["shares"]
        if inferred_shares != actual_shares:
            share_mismatches.append({
                "code": code,
                "name": actual_positions[code].get("name") or inferred_positions[code].get("name", ""),
                "event_shares": inferred_shares,
                "broker_shares": actual_shares,
                "delta_shares": actual_shares - inferred_shares,
            })

    ok = not event_only_codes and not broker_only_codes and not share_mismatches
    return {
        "ok": ok,
        "status": "ok" if ok else "drift",
        "event_trade_count": int(activity.get("trade_count", 0) or 0),
        "inferred_open_codes": inferred_codes,
        "actual_open_codes": actual_codes,
        "event_only_codes": event_only_codes,
        "broker_only_codes": broker_only_codes,
        "share_mismatches": share_mismatches,
        "inferred_positions": inferred_positions,
        "actual_positions": actual_positions,
    }


def reconcile_trade_state(apply: bool = False, window: int = 180) -> dict:
    snapshot = paper_trade_consistency_snapshot(window=window)
    actions = []

    for code in snapshot.get("event_only_codes", []):
        inferred = snapshot["inferred_positions"].get(code, {})
        shares = _safe_int(inferred.get("shares", 0))
        if shares <= 0:
            continue
        reference_price = _safe_float(
            inferred.get("avg_cost") or inferred.get("last_buy_price") or inferred.get("first_buy_price"),
            0.0,
        )
        actions.append({
            "action": "flatten_missing_broker_position",
            "side": "sell",
            "code": code,
            "name": inferred.get("name", code),
            "shares": shares,
            "price": reference_price,
            "reason_code": "PAPER_RECONCILE_FLATTEN",
            "reason": (
                f"对账平仓：broker 实际空仓，但事件流残留 {shares} 股。"
                "以参考成本价补录中性卖出，真实已实现盈亏未知。"
            ),
        })

    for code in snapshot.get("broker_only_codes", []):
        actual = snapshot["actual_positions"].get(code, {})
        shares = _safe_int(actual.get("shares", 0))
        if shares <= 0:
            continue
        actions.append({
            "action": "open_missing_event_position",
            "side": "buy",
            "code": code,
            "name": actual.get("name", code),
            "shares": shares,
            "price": _safe_float(actual.get("cost", 0.0), 0.0),
            "reason_code": "PAPER_RECONCILE_OPEN",
            "reason": (
                f"对账开仓：broker 持有 {shares} 股，但事件流缺失。"
                "以当前持仓成本补录基线买入。"
            ),
        })

    for mismatch in snapshot.get("share_mismatches", []):
        delta = _safe_int(mismatch.get("delta_shares", 0))
        code = mismatch.get("code", "")
        if not code or delta == 0:
            continue
        if delta > 0:
            actual = snapshot["actual_positions"].get(code, {})
            actions.append({
                "action": "increase_event_position_to_broker",
                "side": "buy",
                "code": code,
                "name": mismatch.get("name", code),
                "shares": delta,
                "price": _safe_float(actual.get("cost", 0.0), 0.0),
                "reason_code": "PAPER_RECONCILE_ADD",
                "reason": (
                    f"对账补仓：broker {mismatch.get('broker_shares', 0)} 股，"
                    f"事件流仅 {mismatch.get('event_shares', 0)} 股。补录 {delta} 股买入。"
                ),
            })
        else:
            inferred = snapshot["inferred_positions"].get(code, {})
            actions.append({
                "action": "decrease_event_position_to_broker",
                "side": "sell",
                "code": code,
                "name": mismatch.get("name", code),
                "shares": abs(delta),
                "price": _safe_float(
                    inferred.get("avg_cost") or inferred.get("first_buy_price"),
                    0.0,
                ),
                "reason_code": "PAPER_RECONCILE_REDUCE",
                "reason": (
                    f"对账减仓：broker {mismatch.get('broker_shares', 0)} 股，"
                    f"事件流 {mismatch.get('event_shares', 0)} 股。补录 {abs(delta)} 股卖出。"
                    "真实已实现盈亏未知。"
                ),
            })

    result = {
        "status": snapshot.get("status", "ok"),
        "apply": apply,
        "consistency_before": snapshot,
        "planned_actions": actions,
        "planned_action_count": len(actions),
        "applied_actions": [],
    }

    if apply and actions:
        for item in actions:
            action_text = "买入" if item["side"] == "buy" else "卖出"
            _log_trade(
                action_text,
                item["code"],
                item["name"],
                item["shares"],
                item["price"],
                item["reason"],
                item["reason_code"],
                source="paper_reconcile",
                metadata={"reconcile_action": item["action"]},
            )
            result["applied_actions"].append(item)
        result["consistency_after"] = paper_trade_consistency_snapshot(window=window)
        result["status"] = result["consistency_after"].get("status", "ok")
    else:
        result["consistency_after"] = snapshot

    return result


def _get_positions(_client: object = None) -> list:
    """获取模拟盘当前持仓列表"""
    if _client is not None and hasattr(_client, "positions"):
        result = _client.positions()
    else:
        result = _query_mx("mx.moni.positions", fallback={})
    if not isinstance(result, dict):
        return []
    data = result.get("data", {})
    if not isinstance(data, dict):
        return []
    return data.get("posList", [])


def _get_balance(_client: object = None) -> dict:
    """获取模拟盘资金信息"""
    if _client is not None and hasattr(_client, "balance"):
        result = _client.balance()
    else:
        result = _query_mx("mx.moni.balance", fallback={})
    if not isinstance(result, dict):
        result = {}
    data = result.get("data", {})
    if not isinstance(data, dict):
        data = {}
    return {
        "total_assets": data.get("totalAssets", 0),
        "available": data.get("availBalance", 0),
        "position_value": data.get("totalPosValue", 0),
        "total_profit": data.get("totalProfit", 0),
        "init_money": data.get("initMoney", 200000),
    }


def _get_orders(_client: object = None) -> list:
    """获取模拟盘委托/成交记录"""
    if _client is not None and hasattr(_client, "orders"):
        result = _client.orders()
    else:
        result = _query_mx("mx.moni.orders", fallback={})
    if not isinstance(result, dict):
        return []
    data = result.get("data", {})
    if not isinstance(data, dict):
        return []
    return data.get("orderList", data.get("list", []))


def _calc_shares(price: float, amount: float = POSITION_SIZE) -> int:
    """根据目标金额和价格计算买入股数（100的整数倍）"""
    if price <= 0:
        return 0
    shares = int(amount / price)
    shares = (shares // 100) * 100
    return max(shares, 100)


# ---------------------------------------------------------------------------
# 买入：核心池新股票
# ---------------------------------------------------------------------------

def buy_new_picks(dry_run: bool = False) -> list:
    """
    对核心池中的股票执行模拟盘买入。
    已持有的跳过，只买新入池的。

    Returns:
        list of {"code": str, "name": str, "shares": int, "status": str}
    """
    stocks_cfg = get_stocks()
    core_pool = stocks_cfg.get("core_pool", [])

    if not core_pool:
        _logger.info("[shadow] 核心池为空，无需买入")
        return []

    # 获取当前持仓代码
    positions = _get_positions()
    held_codes = set()
    for pos in positions:
        code = str(pos.get("stockCode", pos.get("secuCode", ""))).strip()
        if code:
            held_codes.add(code)

    # 获取可用资金
    balance = _get_balance()
    available = balance["available"]
    _logger.info(f"[shadow] 可用资金: ¥{available:,.0f}  持仓: {len(held_codes)} 只")
    strategy = get_strategy()
    buy_threshold = strategy.get("scoring", {}).get("thresholds", {}).get("buy", 7)

    results = []
    for item in core_pool:
        code = str(item.get("code", "")).strip()
        name = str(item.get("name", "")).strip()

        if not code or not name:
            continue

        try:
            score_result = score_stock(code, name)
        except Exception as e:
            _logger.warning(f"[shadow] {name}({code}) 评分失败，跳过: {e}")
            results.append(_trade_result(code, name, 0, "评分失败", str(e), "SCORE_ERROR"))
            continue

        total_score = float(score_result.get("total_score", 0) or 0)
        if data_quality_blocks_auto_buy(score_result):
            data_quality = normalize_data_quality(score_result.get("data_quality", "ok"))
            missing_fields = score_result.get("data_missing_fields", score_result.get("missing_fields", []))
            reason = data_quality_review_reason(score_result)
            reason_code = "DATA_QUALITY_BLOCKED" if data_quality == "error" else "DATA_QUALITY_MANUAL_REVIEW"
            status = "blocked" if data_quality == "error" else "人工复核"
            _logger.info(f"[shadow] {name}({code}) {reason}，跳过自动买入")
            results.append(
                _trade_result(
                    code,
                    name,
                    0,
                    status,
                    reason,
                    reason_code,
                    score=total_score,
                    data_quality=data_quality,
                    data_missing_fields=missing_fields,
                )
            )
            continue
        veto_signals = score_result.get("veto_signals", [])
        if veto_signals:
            reason_code = "POOL_VETO"
            reason = f"veto:{','.join(veto_signals)}"
            _logger.info(f"[shadow] {name}({code}) 触发一票否决，跳过: {reason}")
            results.append(_trade_result(code, name, 0, "跳过", reason, reason_code, score=total_score))
            continue
        if total_score < buy_threshold:
            _logger.info(f"[shadow] {name}({code}) 分数{total_score:.1f}<{buy_threshold}，跳过")
            results.append(_trade_result(code, name, 0, "跳过", f"分数不足:{total_score:.1f}", "SCORE_TOO_LOW", score=total_score))
            continue

        if code in held_codes:
            _logger.info(f"[shadow] {name}({code}) 已持有，跳过")
            results.append(_trade_result(code, name, 0, "跳过", "已持有", "POSITION_HELD"))
            continue

        if available < POSITION_SIZE * 0.5:
            _logger.warning(f"[shadow] 可用资金不足 ¥{available:,.0f}，停止买入")
            results.append(_trade_result(code, name, 0, "跳过", "资金不足", "CAPITAL_INSUFFICIENT"))
            continue

        # 用 MX 查最新价
        try:
            price_result = _query_mx("mx.data.query", query=f"{code}最新价", fallback={})
            dto_list = price_result.get("data", {}).get("data", {}).get(
                "searchDataResultDTO", {}).get("dataTableDTOList", [])
            price = 0
            if dto_list:
                table = dto_list[0].get("table", {})
                keys = [k for k in table.keys() if k != "headName"]
                if keys:
                    vals = table[keys[0]]
                    if vals:
                        price = float(str(vals[0]).replace("元", "").replace(",", ""))
        except Exception:
            price = 0

        if price <= 0:
            _logger.warning(f"[shadow] {name}({code}) 无法获取价格，跳过")
            results.append(_trade_result(code, name, 0, "跳过", "无价格", "PRICE_UNAVAILABLE"))
            continue

        shares = _calc_shares(price)
        actual_cost = shares * price

        if dry_run:
            _logger.info(f"[shadow][DRY] 买入 {name}({code}) {shares}股 @ ¥{price:.2f} ≈ ¥{actual_cost:,.0f}")
            results.append(_trade_result(code, name, shares, "dry_run", "模拟运行", "DRY_RUN", price=price))
            continue

        # 执行市价买入
        _logger.info(f"[shadow] 买入 {name}({code}) {shares}股 @ ¥{price:.2f}")
        trade_result, order_state = _submit_shadow_order(
            side="buy",
            code=code,
            name=name,
            shares=shares,
            reason=f"核心池评分{total_score:.1f}",
            reason_code="BUY_CORE_POOL",
            price=price,
            use_market_price=True,
            order_class="manual",
        )
        trade_code = str(trade_result.get("code", ""))
        trade_msg = trade_result.get("message", "")

        if trade_code == "200":
            available -= actual_cost
            held_codes.add(code)
            _logger.info(f"[shadow] ✅ {name} 买入成功 {shares}股")
            results.append(
                _trade_result(
                    code,
                    name,
                    shares,
                    "成功",
                    f"核心池评分{total_score:.1f}",
                    "BUY_CORE_POOL",
                    price=price,
                    order_status=order_state.get("status", ""),
                    order_external_id=order_state.get("external_id", ""),
                )
            )
        else:
            _logger.warning(f"[shadow] ❌ {name} 买入失败: {trade_code} {trade_msg}")
            results.append(
                _trade_result(
                    code,
                    name,
                    0,
                    "失败",
                    trade_msg,
                    "BROKER_REJECTED",
                    order_status=order_state.get("status", ""),
                    order_external_id=order_state.get("external_id", ""),
                )
            )

    return results


# ---------------------------------------------------------------------------
# 止损止盈检查
# ---------------------------------------------------------------------------

def check_stop_signals(dry_run: bool = False) -> list:
    """
    检查模拟盘持仓是否触发止损/止盈信号，盘中执行卖出。

    本波仅自动执行：
      - 动态止损
      - 绝对止损
      - 第一批止盈
    时间止损与回撤止盈仅作为提示，不自动下单。

    调用时机：
      - morning.py 盘前（8:25）→ 只计算价格，不下单（盘前无法交易）
      - noon.py 午休（11:55）→ 盘中检查，触发则市价卖出
      - evening.py 收盘（15:35）→ 最后一次检查，触发则市价卖出

    Returns:
        list of {"code": str, "name": str, "action": str, "reason": str}
    """
    from datetime import time as dt_time

    strategy = get_strategy()
    risk_cfg = strategy.get("risk", {})
    stop_loss_pct = risk_cfg.get("stop_loss", 0.04)
    absolute_stop_pct = risk_cfg.get("absolute_stop", 0.07)
    t1_pct = risk_cfg.get("take_profit", {}).get("t1_pct", 0.15)
    paper_position_context = _load_paper_position_context(window=180)
    history_cache = {}
    today = datetime.now().date()

    positions = _get_positions()
    if not positions:
        _logger.info("[shadow] 模拟盘空仓，无需检查")
        return []

    _logger.info(f"[shadow] {AUTOMATION_SCOPE_NOTE}")

    # 判断是否在交易时间（可以下单）
    now = datetime.now()
    current_time = now.time()
    can_trade = (
        now.weekday() < 5 and (
            dt_time(9, 30) <= current_time <= dt_time(11, 30) or
            dt_time(13, 0) <= current_time <= dt_time(15, 0)
        )
    )

    results = []
    for pos in positions:
        position_view = _build_shadow_position_view(
            pos,
            risk_cfg,
            trade_context_map=paper_position_context,
            history_cache=history_cache,
            today=today,
        )
        code = position_view["code"]
        name = position_view["name"]
        shares = int(position_view["shares"])
        cost = float(position_view["cost"])
        price = float(position_view["price"])
        avail_shares = int(pos.get("enableQty", pos.get("availQty", shares)))
        advisory_signals = position_view.get("advisory_signals", [])
        advisory_summary = position_view.get("advisory_summary", "")

        if shares <= 0 or cost <= 0:
            continue

        pnl_pct = (price / cost - 1)

        # 计算止损止盈价格
        stop_loss_price = round(cost * (1 - stop_loss_pct), 2)
        absolute_stop_price = round(cost * (1 - absolute_stop_pct), 2)
        t1_price = round(cost * (1 + t1_pct), 2)

        action = None
        reason = None
        reason_code = None
        sell_price = None

        # 绝对止损
        if pnl_pct <= -absolute_stop_pct:
            action = "清仓"
            reason = f"绝对止损 现价¥{price:.2f} < ¥{absolute_stop_price:.2f} ({pnl_pct*100:+.1f}%)"
            reason_code = "RISK_ABSOLUTE_STOP"
            sell_price = absolute_stop_price
        # 动态止损
        elif pnl_pct <= -stop_loss_pct:
            action = "清仓"
            reason = f"动态止损 现价¥{price:.2f} < ¥{stop_loss_price:.2f} ({pnl_pct*100:+.1f}%)"
            reason_code = "RISK_DYNAMIC_STOP"
            sell_price = stop_loss_price
        # 第一批止盈
        elif pnl_pct >= t1_pct:
            sell_shares = (avail_shares // 4 // 100) * 100
            if sell_shares >= 100:
                action = f"卖出{sell_shares}股"
                reason = f"止盈第一批 现价¥{price:.2f} > ¥{t1_price:.2f} ({pnl_pct*100:+.1f}%)"
                reason_code = "RISK_TAKE_PROFIT_T1"
                sell_price = t1_price

        if not action:
            reason = f"盈亏{pnl_pct*100:+.1f}% 止损¥{stop_loss_price} 止盈¥{t1_price}"
            if advisory_summary:
                reason = f"{reason} | 提示: {advisory_summary}"
            _logger.info(
                f"[shadow] {name}({code}) 现价¥{price:.2f} 成本¥{cost:.2f} "
                f"盈亏{pnl_pct*100:+.1f}% | 止损¥{stop_loss_price} 止盈¥{t1_price} → 持有"
            )
            results.append({
                "code": code,
                "name": name,
                "action": "持有",
                "reason": reason,
                "reason_code": "RISK_HOLD",
                "stop_loss": stop_loss_price,
                "take_profit": t1_price,
                "open_date": position_view.get("open_date", ""),
                "hold_days": position_view.get("hold_days"),
                "advisory_signals": advisory_signals,
                "advisory_summary": advisory_summary,
                "automated_rules": list(AUTOMATED_RISK_RULES.keys()),
                "advisory_rules": list(ADVISORY_RISK_RULES.keys()),
            })
            continue

        _logger.info(f"[shadow] {name}({code}) → {action} [{reason_code}] ({reason})")
        if advisory_summary:
            _logger.info(f"[shadow] {name}({code}) advisory: {advisory_summary}")

        if dry_run or not can_trade:
            tag = "dry_run" if dry_run else "非交易时间"
            _logger.info(f"[shadow] [{tag}] 不下单，记录信号待下次盘中执行")
            results.append({
                "code": code,
                "name": name,
                "action": action,
                "reason": reason,
                "reason_code": reason_code,
                "status": tag,
                "stop_loss": stop_loss_price,
                "take_profit": t1_price,
                "open_date": position_view.get("open_date", ""),
                "hold_days": position_view.get("hold_days"),
                "advisory_signals": advisory_signals,
                "advisory_summary": advisory_summary,
                "automated_rules": list(AUTOMATED_RISK_RULES.keys()),
                "advisory_rules": list(ADVISORY_RISK_RULES.keys()),
            })
            continue

        # 盘中执行：限价卖出（止损用止损价，止盈用止盈价）
        if action == "清仓":
            sell_qty = (avail_shares // 100) * 100
        else:
            sell_qty = int(action.replace("卖出", "").replace("股", ""))

        if sell_qty < 100:
            results.append({
                "code": code,
                "name": name,
                "action": action,
                "reason": reason,
                "reason_code": reason_code,
                "status": "不足100股",
                "advisory_signals": advisory_signals,
                "advisory_summary": advisory_summary,
                "automated_rules": list(AUTOMATED_RISK_RULES.keys()),
                "advisory_rules": list(ADVISORY_RISK_RULES.keys()),
            })
            continue

        # 止损用市价确保成交，止盈用限价锁定利润
        use_market_price = "止损" in reason
        trade_result, order_state = _submit_shadow_order(
            side="sell",
            code=code,
            name=name,
            shares=sell_qty,
            reason=reason,
            reason_code=reason_code or "",
            price=sell_price or price,
            use_market_price=use_market_price,
            order_class="risk",
        )
        trade_code = str(trade_result.get("code", ""))
        if trade_code == "200":
            if order_state.get("status") == "filled":
                _logger.info(f"[shadow] ✅ {name} {action} 成功")
            else:
                _logger.info(f"[shadow] ✅ {name} {action} 已挂单")
            results.append({
                "code": code,
                "name": name,
                "action": action,
                "reason": reason,
                "reason_code": reason_code,
                "status": "成功" if order_state.get("status") == "filled" else "挂单",
                "order_status": order_state.get("status", ""),
                "order_external_id": order_state.get("external_id", ""),
                "advisory_signals": advisory_signals,
                "advisory_summary": advisory_summary,
                "automated_rules": list(AUTOMATED_RISK_RULES.keys()),
                "advisory_rules": list(ADVISORY_RISK_RULES.keys()),
            })
        else:
            msg = trade_result.get("message", "")
            _logger.warning(f"[shadow] ❌ {name} {action} 失败: {msg}")
            results.append({
                "code": code,
                "name": name,
                "action": action,
                "reason": reason,
                "reason_code": reason_code,
                "status": f"失败:{msg}",
                "advisory_signals": advisory_signals,
                "advisory_summary": advisory_summary,
                "automated_rules": list(AUTOMATED_RISK_RULES.keys()),
                "advisory_rules": list(ADVISORY_RISK_RULES.keys()),
            })

    return results


# ---------------------------------------------------------------------------
# 状态查询
# ---------------------------------------------------------------------------

def get_status() -> dict:
    """获取模拟盘完整状态"""
    balance = _get_balance()
    positions = _get_positions()
    order_sync = _sync_broker_orders()
    order_snapshot = load_order_snapshot(scope="paper_mx")
    risk_cfg = get_strategy().get("risk", {})
    paper_position_context = _load_paper_position_context(window=180)
    history_cache = {}
    today = datetime.now().date()
    mx_health = _mx_health_snapshot(include_unavailable=True)

    pos_list = []
    for pos in positions:
        pos_list.append(
            _build_shadow_position_view(
                pos,
                risk_cfg,
                trade_context_map=paper_position_context,
                history_cache=history_cache,
                today=today,
            )
        )

    advisory_positions = []
    triggered_rules = set()
    triggered_signal_count = 0
    for position in pos_list:
        signals = position.get("advisory_signals", [])
        if not signals:
            continue
        triggered_signal_count += len(signals)
        triggered_rules.update(signal.get("rule_code", "") for signal in signals if signal.get("rule_code"))
        advisory_positions.append({
            "code": position.get("code", ""),
            "name": position.get("name", ""),
            "open_date": position.get("open_date", ""),
            "hold_days": position.get("hold_days"),
            "peak_close": position.get("peak_close", 0.0),
            "peak_date": position.get("peak_date", ""),
            "drawdown_pct": position.get("drawdown_pct", 0.0),
            "signals": signals,
            "summary": position.get("advisory_summary", ""),
        })

    return {
        "balance": balance,
        "positions": pos_list,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "automation_scope": AUTOMATION_SCOPE_NOTE,
        "automated_rules": list(AUTOMATED_RISK_RULES.keys()),
        "advisory_rules": list(ADVISORY_RISK_RULES.keys()),
        "advisory_summary": {
            "triggered_signal_count": triggered_signal_count,
            "triggered_position_count": len(advisory_positions),
            "triggered_rules": sorted(triggered_rules),
            "positions": advisory_positions,
        },
        "mx_health": mx_health,
        "orders": order_snapshot.get("orders", []),
        "order_summary": order_snapshot.get("summary", {}),
        "order_sync": order_sync,
    }


# ---------------------------------------------------------------------------
# 报告生成
# ---------------------------------------------------------------------------

def generate_report() -> str:
    """生成模拟盘报告写入 Obsidian"""
    status = get_status()
    bal = status["balance"]
    positions = status["positions"]
    advisory_summary = status.get("advisory_summary", {})
    mx_health = status.get("mx_health", {})

    init = bal.get("init_money", 200000)
    total = bal.get("total_assets", 0)
    total_return = ((total / init) - 1) * 100 if init > 0 else 0

    lines = [
        f"# 模拟盘报告 — {status['timestamp']}",
        "",
        "## 规则范围",
        "",
        f"> {status.get('automation_scope', AUTOMATION_SCOPE_NOTE)}",
        "",
        "## 账户概览",
        "",
        f"| 项目 | 数值 |",
        f"|------|------|",
        f"| 初始资金 | ¥{init:,.0f} |",
        f"| 总资产 | ¥{total:,.0f} |",
        f"| 可用资金 | ¥{bal.get('available', 0):,.0f} |",
        f"| 持仓市值 | ¥{bal.get('position_value', 0):,.0f} |",
        f"| 总收益 | ¥{bal.get('total_profit', 0):,.0f} ({total_return:+.2f}%) |",
        "",
        "## MX 能力状态",
        "",
        f"| 项目 | 数值 |",
        f"|------|------|",
        f"| 状态 | {mx_health.get('status', 'unknown')} |",
        f"| 可用命令 | {mx_health.get('available_count', 0)} |",
        f"| 不可用命令 | {mx_health.get('unavailable_count', 0)} |",
        "",
    ]

    if positions:
        lines.append("## 当前持仓")
        lines.append("")
        lines.append("| 股票 | 代码 | 持仓 | 成本 | 现价 | 市值 | 盈亏 | 盈亏% |")
        lines.append("|------|------|------|------|------|------|------|-------|")
        for p in positions:
            lines.append(
                f"| {p['name']} | {p['code']} | {p['shares']}股 | "
                f"¥{p['cost']:.2f} | ¥{p['price']:.2f} | "
                f"¥{p['market_value']:,.0f} | ¥{p['pnl']:,.0f} | "
                f"{p['pnl_pct']:+.1f}% |"
            )
        lines.append("")
    else:
        lines.append("## 当前持仓：空仓")
        lines.append("")

    lines.extend([
        "## Advisory 风控提示",
        "",
        f"> {AUTOMATION_SCOPE_NOTE}",
        "",
    ])
    if advisory_summary.get("positions"):
        lines.append("| 股票 | 代码 | 持有天数 | 高点 | 回撤 | 提示 |")
        lines.append("|------|------|----------|------|------|------|")
        for item in advisory_summary.get("positions", []):
            peak_close = _safe_float(item.get("peak_close", 0), 0.0)
            peak_date = str(item.get("peak_date", "")).strip()
            peak_text = f"¥{peak_close:.2f}" if peak_close > 0 else "—"
            if peak_text != "—" and peak_date:
                peak_text = f"{peak_text}({peak_date})"
            drawdown_pct = _safe_float(item.get("drawdown_pct", 0), 0.0)
            lines.append(
                f"| {item.get('name', '—')} | {item.get('code', '—')} | "
                f"{item.get('hold_days', '—')} | {peak_text} | "
                f"{drawdown_pct*100:.1f}% | {item.get('summary', '—')} |"
            )
        lines.append("")
    else:
        lines.append("当前无时间止损 / 回撤止盈提示。")
        lines.append("")

    lines.append(f"> 本报告由影子交易引擎自动生成，用于验证交易系统逻辑")

    content = "\n".join(lines)

    # 写入 Obsidian（通过 vault.write 触发自动备份）
    vault = ObsidianVault()
    date_str = datetime.now().strftime("%Y%m%d")
    report_relative = f"{vault.paper_trade_dir}/模拟盘_{date_str}.md"
    vault.write(report_relative, content)

    report_path = f"{vault.vault_path}/{report_relative}"
    _logger.info(f"[shadow] 报告已写入: {report_path}")
    return report_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    import argparse
    parser = argparse.ArgumentParser(description="影子交易引擎（模拟盘验证）")
    parser.add_argument("action", choices=["buy", "check", "status", "report"],
                        help="buy=买入核心池 check=检查止损止盈 status=查看状态 report=生成报告")
    parser.add_argument("--dry-run", action="store_true", help="模拟运行，不实际下单")
    args = parser.parse_args()

    if args.action == "buy":
        results = buy_new_picks(dry_run=args.dry_run)
        print(f"\n影子交易买入: {len(results)} 只")
        for r in results:
            reason_code = r.get("reason_code", "")
            reason = r.get("reason", "")
            detail = f" [{reason_code}] {reason}".strip() if (reason_code or reason) else ""
            print(f"  {r['name']}({r['code']}): {r['status']}{detail}"
                  + (f" {r['shares']}股" if r.get('shares') else ""))

    elif args.action == "check":
        results = check_stop_signals(dry_run=args.dry_run)
        print(f"\n止损止盈检查: {len(results)} 只")
        for r in results:
            reason_code = r.get("reason_code", "")
            prefix = f"[{reason_code}] " if reason_code else ""
            print(f"  {r['name']}({r['code']}): {r['action']} — {prefix}{r['reason']}")

    elif args.action == "status":
        status = get_status()
        bal = status["balance"]
        print(f"\n模拟盘状态 ({status['timestamp']})")
        print(f"  总资产: ¥{bal['total_assets']:,.0f}  可用: ¥{bal['available']:,.0f}")
        print(f"  持仓市值: ¥{bal['position_value']:,.0f}  总收益: ¥{bal['total_profit']:,.0f}")
        print(f"  自动规则: {status.get('automation_scope', AUTOMATION_SCOPE_NOTE)}")
        if status["positions"]:
            print(f"\n  持仓 ({len(status['positions'])} 只):")
            for p in status["positions"]:
                print(f"    {p['name']}({p['code']}) {p['shares']}股 "
                      f"成本¥{p['cost']:.2f} 现价¥{p['price']:.2f} "
                      f"盈亏{p['pnl_pct']:+.1f}%")
                if p.get("advisory_signals"):
                    for signal in p["advisory_signals"]:
                        print(f"      advisory [{signal['rule_code']}]: {signal['message']}")
        else:
            print("  持仓: 空仓")

    elif args.action == "report":
        path = generate_report()
        print(f"\n报告已生成: {path}")


if __name__ == "__main__":
    main()
