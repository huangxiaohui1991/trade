from __future__ import annotations

from hermes.market.models import StockSnapshot
from hermes.strategy.continuation_models import ContinuationFilterConfig, ContinuationFilterResult


class ContinuationQualifier:
    def __init__(self, config: ContinuationFilterConfig):
        self.config = config

    def qualify(self, snapshot: StockSnapshot) -> ContinuationFilterResult:
        if not snapshot.quote or not snapshot.technical:
            return ContinuationFilterResult(False, ["missing_quote_or_technical"])

        q = snapshot.quote
        t = snapshot.technical
        close_near_high = 0.0 if q.high <= q.low else (q.close - q.low) / (q.high - q.low)
        intraday_retrace = 0.0 if q.high <= 0 else max(0.0, (q.high - q.close) / q.high)
        reasons: list[str] = []

        if q.amount < self.config.amount_min:
            reasons.append("amount")
        if q.change_pct < self.config.change_pct_min:
            reasons.append("change_pct")
        if close_near_high < self.config.close_near_high_min:
            reasons.append("close_near_high")
        if intraday_retrace > self.config.max_intraday_retrace:
            reasons.append("intraday_retrace")
        if not (self.config.volume_ratio_min <= t.volume_ratio <= self.config.volume_ratio_max):
            reasons.append("volume_ratio")
        if self.config.require_above_ma5 and q.close < t.ma5:
            reasons.append("above_ma5")

        return ContinuationFilterResult(
            qualified=len(reasons) == 0,
            reasons=reasons,
            close_near_high=round(close_near_high, 4),
            intraday_retrace=round(intraday_retrace, 4),
        )
