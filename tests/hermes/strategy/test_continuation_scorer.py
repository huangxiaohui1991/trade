from hermes.market.models import FundFlow, StockQuote, StockSnapshot, TechnicalIndicators
from hermes.strategy.continuation_filters import ContinuationQualifier
from hermes.strategy.continuation_models import (
    ContinuationFilterConfig,
    ContinuationScoreConfig,
    ContinuationScoreResult,
)
from hermes.strategy.continuation_scorer import ContinuationScorer


def test_continuation_configs_expose_expected_defaults():
    filter_cfg = ContinuationFilterConfig()
    score_cfg = ContinuationScoreConfig()

    assert filter_cfg.amount_min == 2e8
    assert filter_cfg.close_near_high_min == 0.75
    assert score_cfg.top_n == 3
    assert score_cfg.hold_days == (1, 2, 3)


def test_continuation_score_result_computes_total_after_penalty():
    result = ContinuationScoreResult(
        code="002138",
        name="双环传动",
        qualified=True,
        strength_score=2.0,
        continuity_score=1.5,
        quality_score=1.0,
        flow_score=0.5,
        stability_score=0.8,
        overheat_penalty=1.2,
        notes=["close_near_high=0.91"],
    )

    assert result.total_score == 4.6


def test_continuation_score_config_normalizes_hold_days_to_tuple():
    score_cfg = ContinuationScoreConfig(hold_days=[1, 2, 3])

    assert score_cfg.hold_days == (1, 2, 3)
    assert isinstance(score_cfg.hold_days, tuple)


def _make_snapshot(**overrides) -> StockSnapshot:
    quote = StockQuote(
        code="002138",
        name="双环传动",
        price=15.0,
        open=14.6,
        high=15.1,
        low=14.5,
        close=15.0,
        volume=5_000_000,
        amount=4e8,
        change_pct=3.2,
    )
    technical = TechnicalIndicators(
        ma5=14.7,
        ma10=14.3,
        ma20=13.9,
        ma60=13.1,
        above_ma20=True,
        volume_ratio=1.9,
        rsi=63.0,
        golden_cross=False,
        momentum_5d=6.0,
        deviation_rate=3.2,
        change_pct=3.2,
    )
    flow = FundFlow(net_inflow_1d=3e8, northbound_net_positive=True)
    payload = dict(code="002138", name="双环传动", quote=quote, technical=technical, flow=flow)
    payload.update(overrides)
    return StockSnapshot(**payload)


def test_scorer_returns_positive_total_for_qualified_candidate():
    snapshot = _make_snapshot()
    qualifier = ContinuationQualifier(ContinuationFilterConfig())
    scorer = ContinuationScorer(ContinuationScoreConfig())

    result = scorer.score(snapshot, qualifier.qualify(snapshot))

    assert result.qualified is True
    assert result.total_score > 0
    assert result.overheat_penalty == 0


def test_scorer_applies_overheat_penalty_to_extended_candidate():
    snapshot = _make_snapshot(
        technical=TechnicalIndicators(
            ma5=14.7,
            ma10=14.3,
            ma20=13.9,
            ma60=13.1,
            above_ma20=True,
            volume_ratio=4.6,
            rsi=76.0,
            golden_cross=False,
            momentum_5d=10.5,
            deviation_rate=9.5,
            change_pct=8.6,
        ),
    )
    qualifier = ContinuationQualifier(ContinuationFilterConfig(volume_ratio_max=5.0))
    scorer = ContinuationScorer(ContinuationScoreConfig())

    result = scorer.score(snapshot, qualifier.qualify(snapshot))

    assert result.overheat_penalty > 0
    assert "overheat" in " ".join(result.notes)
