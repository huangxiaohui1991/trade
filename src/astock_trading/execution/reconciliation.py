"""模拟盘与实盘逐笔对账。"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any

from astock_trading.platform.domain_events import (
    AUTO_TRADE_EXECUTED,
    RULE_DEVIATION_RECORDED,
    TRADE_HYPOTHESIS_RECORDED,
    TRADE_OUTCOME_RECORDED,
)
from astock_trading.platform.events import EventStore
from astock_trading.platform.time import iso_to_local_date_str, local_date_bounds_utc, local_today_str


@dataclass
class ReconcileTrade:
    account: str
    event_id: str
    order_id: str
    code: str
    name: str
    side: str
    shares: int
    price_cents: int
    status: str
    event_date: str
    occurred_at: str
    source_event_id: str = ""
    source_score_event_id: str = ""
    manual_reason: str = ""

    @property
    def signal_id(self) -> str:
        return self.source_score_event_id or self.source_event_id

    def to_dict(self) -> dict[str, Any]:
        return {
            "account": self.account,
            "event_id": self.event_id,
            "order_id": self.order_id,
            "code": self.code,
            "name": self.name,
            "side": self.side,
            "shares": self.shares,
            "price_cents": self.price_cents,
            "price": round(self.price_cents / 100, 2),
            "status": self.status,
            "event_date": self.event_date,
            "occurred_at": self.occurred_at,
            "source_event_id": self.source_event_id,
            "source_score_event_id": self.source_score_event_id,
            "signal_id": self.signal_id,
            "manual_reason": self.manual_reason,
        }


class TradeReconciliationService:
    """对比模拟盘信号与实盘人工执行，输出逐笔偏离。"""

    def __init__(self, event_store: EventStore):
        self._events = event_store

    def reconcile(
        self,
        *,
        date: str | None = None,
        record: bool = False,
        slippage_bps: int = 50,
        limit: int = 1000,
    ) -> dict[str, Any]:
        target_date = date or local_today_str()
        since, until = local_date_bounds_utc(target_date)
        paper = self._paper_trades(since=since, until=until, limit=limit)
        real = self._real_trades(since=since, until=until, limit=limit)

        items = self._join_trades(paper, real, slippage_bps=slippage_bps)
        deviations = [item for item in items if item["deviation_type"] != "matched"]
        recorded_count = 0
        if record:
            for item in deviations:
                if self._record_deviation(item):
                    recorded_count += 1

        return {
            "status": "applied" if record else "dry_run",
            "date": target_date,
            "join_policy": "signal_id 优先；缺失时回退到 code + side + event_date；order_id 进入明细留痕",
            "summary": {
                "paper_trades": len(paper),
                "real_trades": len(real),
                "matched": sum(1 for item in items if item["deviation_type"] == "matched"),
                "deviation_count": len(deviations),
                "deviation_types": _count_by_type(deviations),
            },
            "recorded_count": recorded_count,
            "items": items,
        }

    def _paper_trades(self, *, since: str, until: str, limit: int) -> list[ReconcileTrade]:
        events = self._events.query(
            event_type=AUTO_TRADE_EXECUTED,
            since=since,
            until=until,
            limit=limit,
        )
        trades: list[ReconcileTrade] = []
        for event in events:
            if event.get("metadata", {}).get("account") != "paper":
                continue
            payload = event.get("payload", {})
            side = str(payload.get("side", ""))
            status = str(payload.get("status", ""))
            if side not in {"buy", "sell"} or status not in {"filled", "dry_run"}:
                continue
            trades.append(
                ReconcileTrade(
                    account="paper",
                    event_id=event["event_id"],
                    order_id=str(payload.get("order_id", "")),
                    code=str(payload.get("code", "")),
                    name=str(payload.get("name", payload.get("code", ""))),
                    side=side,
                    shares=int(payload.get("shares", 0) or 0),
                    price_cents=_price_cents(payload),
                    status=status,
                    event_date=_event_date(event),
                    occurred_at=str(event.get("occurred_at", "")),
                    source_event_id=str(payload.get("source_event_id", "")),
                    source_score_event_id=str(payload.get("source_score_event_id", "")),
                )
            )
        return trades

    def _real_trades(self, *, since: str, until: str, limit: int) -> list[ReconcileTrade]:
        outcomes = self._events.query(
            event_type=TRADE_OUTCOME_RECORDED,
            since=since,
            until=until,
            limit=limit,
        )
        hypotheses = {
            event.get("payload", {}).get("order_id"): event.get("payload", {})
            for event in self._events.query(event_type=TRADE_HYPOTHESIS_RECORDED, limit=limit)
        }
        trades: list[ReconcileTrade] = []
        for event in outcomes:
            metadata = event.get("metadata", {})
            if metadata.get("account") != "main" or metadata.get("execution") != "manual":
                continue
            payload = event.get("payload", {})
            hypothesis = hypotheses.get(payload.get("order_id"), {}).get("hypothesis", {})
            trades.append(
                ReconcileTrade(
                    account="real",
                    event_id=event["event_id"],
                    order_id=str(payload.get("order_id", "")),
                    code=str(payload.get("code", "")),
                    name=str(payload.get("name", payload.get("code", ""))),
                    side=str(payload.get("side", "")),
                    shares=int(payload.get("shares", 0) or 0),
                    price_cents=_price_cents(payload),
                    status=str(payload.get("status", "")),
                    event_date=_event_date(event),
                    occurred_at=str(event.get("occurred_at", "")),
                    source_event_id=str(payload.get("source_event_id", "")),
                    source_score_event_id=str(payload.get("source_score_event_id", "")),
                    manual_reason=str(hypothesis.get("manual_reason") or payload.get("reason", "")),
                )
            )
        return trades

    def _join_trades(
        self,
        paper: list[ReconcileTrade],
        real: list[ReconcileTrade],
        *,
        slippage_bps: int,
    ) -> list[dict[str, Any]]:
        unmatched_real = list(real)
        items: list[dict[str, Any]] = []

        for paper_trade in paper:
            real_trade = _pop_match(paper_trade, unmatched_real)
            items.append(_build_item(paper_trade, real_trade, slippage_bps=slippage_bps))

        for real_trade in unmatched_real:
            items.append(_build_item(None, real_trade, slippage_bps=slippage_bps))

        return sorted(items, key=lambda item: (
            item["join_key"].get("event_date", ""),
            item["join_key"].get("code", ""),
            item["join_key"].get("side", ""),
            item["deviation_type"],
        ))

    def _record_deviation(self, item: dict[str, Any]) -> bool:
        deviation_id = item["deviation_id"]
        code = item["join_key"].get("code", "unknown")
        event_date = item["join_key"].get("event_date", "unknown")
        stream = f"rule_deviation:{event_date}:{code}"
        existing = self._events.query(stream=stream, event_type=RULE_DEVIATION_RECORDED, limit=200)
        if any(event.get("payload", {}).get("deviation_id") == deviation_id for event in existing):
            return False
        self._events.append(
            stream=stream,
            stream_type="rule_deviation",
            event_type=RULE_DEVIATION_RECORDED,
            payload={
                "deviation_id": deviation_id,
                "rule_deviation": "shadow_divergence",
                "deviation_type": item["deviation_type"],
                "join_key": item["join_key"],
                "paper": item["paper"],
                "real": item["real"],
                "details": item["details"],
            },
            metadata={"source": "review.shadow", "account": "main"},
        )
        return True


def _pop_match(paper_trade: ReconcileTrade, real_trades: list[ReconcileTrade]) -> ReconcileTrade | None:
    if paper_trade.signal_id:
        for index, real_trade in enumerate(real_trades):
            if (
                real_trade.signal_id == paper_trade.signal_id
                and real_trade.code == paper_trade.code
                and real_trade.side == paper_trade.side
            ):
                return real_trades.pop(index)

    for index, real_trade in enumerate(real_trades):
        if (
            real_trade.code == paper_trade.code
            and real_trade.side == paper_trade.side
            and real_trade.event_date == paper_trade.event_date
        ):
            return real_trades.pop(index)
    return None


def _build_item(
    paper: ReconcileTrade | None,
    real: ReconcileTrade | None,
    *,
    slippage_bps: int,
) -> dict[str, Any]:
    deviation_type, details = _classify(paper, real, slippage_bps=slippage_bps)
    join_key = {
        "signal_id": (paper.signal_id if paper else "") or (real.signal_id if real else ""),
        "code": (paper.code if paper else "") or (real.code if real else ""),
        "side": (paper.side if paper else "") or (real.side if real else ""),
        "event_date": (paper.event_date if paper else "") or (real.event_date if real else ""),
        "order_id": (real.order_id if real else "") or (paper.order_id if paper else ""),
    }
    payload = {
        "join_key": join_key,
        "deviation_type": deviation_type,
        "rule_deviation": "" if deviation_type == "matched" else "shadow_divergence",
        "paper": paper.to_dict() if paper else None,
        "real": real.to_dict() if real else None,
        "details": details,
    }
    payload["deviation_id"] = _deviation_id(payload)
    return payload


def _classify(
    paper: ReconcileTrade | None,
    real: ReconcileTrade | None,
    *,
    slippage_bps: int,
) -> tuple[str, dict[str, Any]]:
    if paper is not None and real is None:
        return "not_executed", {"message": "模拟盘有信号/成交，实盘未执行"}
    if paper is None and real is not None:
        return "extra_real_trade", {"message": "实盘有人工记录，模拟盘无对应信号"}
    if paper is None or real is None:
        return "unknown", {"message": "无法识别对账状态"}

    secondary: list[str] = []
    if paper.price_cents > 0 and real.price_cents > 0:
        slippage = abs(real.price_cents - paper.price_cents) / paper.price_cents * 10000
    else:
        slippage = 0.0
    details = {
        "paper_shares": paper.shares,
        "real_shares": real.shares,
        "paper_price_cents": paper.price_cents,
        "real_price_cents": real.price_cents,
        "price_slippage_bps": round(slippage, 2),
        "secondary_deviations": secondary,
    }

    if paper.shares != real.shares:
        if slippage > slippage_bps:
            secondary.append("price_slippage")
        return "partial_fill", details

    if slippage > slippage_bps:
        return "price_slippage", details

    reason = real.manual_reason.lower()
    if "manual_override" in reason or "override" in reason or "人工调整" in real.manual_reason:
        return "manual_override", details

    return "matched", details


def _price_cents(payload: dict[str, Any]) -> int:
    for key in ("fill_price_cents", "price_cents"):
        value = payload.get(key)
        if value:
            return int(value)
    value = payload.get("price", 0)
    try:
        return int(round(float(value) * 100))
    except (TypeError, ValueError):
        return 0


def _event_date(event: dict[str, Any]) -> str:
    occurred_at = str(event.get("occurred_at", ""))
    if not occurred_at:
        return ""
    try:
        return iso_to_local_date_str(occurred_at)
    except ValueError:
        return occurred_at[:10]


def _count_by_type(items: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        deviation_type = item["deviation_type"]
        counts[deviation_type] = counts.get(deviation_type, 0) + 1
    return dict(sorted(counts.items()))


def _deviation_id(item: dict[str, Any]) -> str:
    parts = [
        item["deviation_type"],
        item["join_key"].get("signal_id", ""),
        item["join_key"].get("code", ""),
        item["join_key"].get("side", ""),
        item["join_key"].get("event_date", ""),
        item["paper"].get("event_id", "") if item.get("paper") else "",
        item["real"].get("order_id", "") if item.get("real") else "",
    ]
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()[:16]
