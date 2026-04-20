from hermes.strategy.continuation_models import (
    ContinuationFilterConfig,
    ContinuationScoreConfig,
    ContinuationScoreResult,
)


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
