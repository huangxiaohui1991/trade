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

        strength = min(2.0, max(0.0, q.change_pct / 2.0))
        continuity = min(1.5, max(0.0, t.momentum_5d / 4.0))
        quality = min(1.5, max(0.0, 1.5 - filter_result.intraday_retrace * 10))
        flow_score = 0.5 if snapshot.flow and snapshot.flow.net_inflow_1d > 0 else 0.0
        stability = 0.7 if q.close >= t.ma5 else 0.0

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
