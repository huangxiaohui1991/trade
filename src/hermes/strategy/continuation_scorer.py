from __future__ import annotations

from hermes.market.models import StockSnapshot
from hermes.strategy.continuation_models import (
    ContinuationFilterResult,
    ContinuationScoreConfig,
    ContinuationScoreResult,
)


class ContinuationScorer:
    def __init__(self, config: ContinuationScoreConfig):
        self.config = config

    def score(
        self, snapshot: StockSnapshot, filter_result: ContinuationFilterResult
    ) -> ContinuationScoreResult:
        if not filter_result.qualified or not snapshot.quote or not snapshot.technical:
            return ContinuationScoreResult(
                code=snapshot.code,
                name=snapshot.name,
                qualified=False,
            )

        q = snapshot.quote
        t = snapshot.technical

        body_ratio = _body_ratio(q)

        strength_signal = (
            0.7
            * _scale(
                q.change_pct,
                self.config.strength_change_floor,
                self.config.strength_change_full,
            )
            + 0.3
            * _scale(
                filter_result.close_near_high,
                self.config.strength_close_near_high_floor,
                self.config.strength_close_near_high_full,
            )
        )
        strength = self.config.strength_weight * strength_signal

        continuity_signal = _scale(
            t.momentum_5d,
            self.config.continuity_momentum_floor,
            self.config.continuity_momentum_full,
        )
        if q.close >= t.ma10 > 0:
            continuity_signal = min(1.0, continuity_signal + 0.15)
        continuity = self.config.continuity_weight * continuity_signal

        retrace_score = _inverse_scale(
            filter_result.intraday_retrace,
            self.config.quality_retrace_good,
            self.config.quality_retrace_bad,
        )
        body_score = _scale(
            body_ratio,
            self.config.quality_body_ratio_floor,
            self.config.quality_body_ratio_full,
        )
        quality = self.config.quality_weight * (0.6 * retrace_score + 0.4 * body_score)

        inflow = snapshot.flow.net_inflow_1d if snapshot.flow else 0.0
        flow_signal = 0.0
        if inflow > 0:
            flow_signal = min(1.0, inflow / self.config.flow_inflow_full)
            if snapshot.flow and snapshot.flow.northbound_net_positive:
                flow_signal = min(1.0, flow_signal + 0.15)
        flow_score = self.config.flow_weight * flow_signal

        stability_signal = 1.0 if q.close >= t.ma5 else 0.0
        if q.close >= t.ma10 > 0:
            stability_signal = min(1.0, stability_signal + 0.1)
        if t.rsi > self.config.stability_rsi_ceiling:
            rsi_drag = _scale(
                t.rsi,
                self.config.stability_rsi_ceiling,
                self.config.stability_rsi_fail,
            )
            stability_signal = max(0.0, stability_signal - 0.6 * rsi_drag)
        stability = self.config.stability_weight * min(1.0, stability_signal)

        penalty = 0.0
        notes: list[str] = []
        if q.change_pct >= self.config.overheat_change_pct:
            penalty += 0.7
            notes.append("overheat:change_pct")
        if t.volume_ratio >= self.config.overheat_volume_ratio:
            penalty += 0.7
            notes.append("overheat:volume_ratio")
        if t.deviation_rate >= self.config.overheat_deviation_rate:
            penalty += 0.6
            notes.append("overheat:deviation_rate")
        if t.rsi >= self.config.overheat_rsi:
            penalty += 0.5
            notes.append("overheat:rsi")

        return ContinuationScoreResult(
            code=snapshot.code,
            name=snapshot.name,
            qualified=True,
            trade_date="",
            strength_score=round(strength, 2),
            continuity_score=round(continuity, 2),
            quality_score=round(quality, 2),
            flow_score=round(flow_score, 2),
            stability_score=round(stability, 2),
            overheat_penalty=round(penalty, 2),
            notes=notes,
        )


def _scale(value: float, floor: float, full: float) -> float:
    if full <= floor:
        return 1.0 if value >= full else 0.0
    if value <= floor:
        return 0.0
    if value >= full:
        return 1.0
    return (value - floor) / (full - floor)


def _inverse_scale(value: float, good: float, bad: float) -> float:
    if bad <= good:
        return 1.0 if value <= good else 0.0
    if value <= good:
        return 1.0
    if value >= bad:
        return 0.0
    return 1.0 - ((value - good) / (bad - good))


def _body_ratio(quote) -> float:
    spread = quote.high - quote.low
    if spread <= 0:
        return 1.0
    return abs(quote.close - quote.open) / spread
