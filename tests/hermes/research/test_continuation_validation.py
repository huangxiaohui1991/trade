import json

import pandas as pd

from hermes.research.continuation_validation import (
    build_score_bucket_report,
    build_top_n_report,
    run_continuation_validation,
)
from hermes.strategy.continuation_models import ContinuationScoreResult


def test_score_bucket_report_groups_candidates_by_score():
    rows = [
        ContinuationScoreResult(code="A", name="A", qualified=True, strength_score=2.0, continuity_score=1.0, quality_score=1.0),
        ContinuationScoreResult(code="B", name="B", qualified=True, strength_score=0.5, continuity_score=0.3, quality_score=0.2),
    ]
    forward_returns = pd.DataFrame(
        [
            {"code": "A", "t1_return": 0.03, "t2_return": 0.05, "t3_return": 0.04},
            {"code": "B", "t1_return": -0.01, "t2_return": 0.00, "t3_return": -0.02},
        ]
    )

    report = build_score_bucket_report(rows, forward_returns, bucket_count=2)

    assert len(report) == 2
    assert report[0]["sample_count"] == 1
    assert "t1_win_rate" in report[0]


def test_top_n_report_aggregates_daily_ranked_returns():
    ranked = pd.DataFrame(
        [
            {"trade_date": "2026-04-01", "code": "A", "rank": 1, "t1_return": 0.03},
            {"trade_date": "2026-04-01", "code": "B", "rank": 2, "t1_return": 0.01},
            {"trade_date": "2026-04-02", "code": "C", "rank": 1, "t1_return": 0.02},
        ]
    )

    report = build_top_n_report(ranked, top_ns=(1, 2))

    assert report[0]["top_n"] == 1
    assert report[0]["avg_t1_return"] > 0


def test_run_continuation_validation_returns_bucket_and_top_n_sections(tmp_path):
    ranked = pd.DataFrame(
        [
            {
                "trade_date": "2026-01-01",
                "code": "600036",
                "rank": 1,
                "t1_return": 0.02,
                "open_t1_return": 0.02,
                "vwap_30m_t1_return": 0.018,
                "open_not_chase_t1_return": 0.015,
            }
        ]
    )
    forward = pd.DataFrame(
        [{"code": "600036", "t1_return": 0.02, "t2_return": 0.03, "t3_return": 0.01}]
    )
    results = [
        {
            "code": "600036",
            "name": "招商银行",
            "qualified": True,
            "strength_score": 1.8,
            "continuity_score": 1.0,
            "quality_score": 1.2,
            "flow_score": 0.4,
            "stability_score": 0.7,
            "overheat_penalty": 0.0,
            "notes": [],
        }
    ]
    ranked.to_csv(tmp_path / "ranked_returns.csv", index=False)
    forward.to_csv(tmp_path / "forward_returns.csv", index=False)
    (tmp_path / "results.json").write_text(json.dumps(results), encoding="utf-8")

    result = run_continuation_validation(
        codes=["600036", "000001"],
        start="2026-01-01",
        end="2026-02-28",
        top_n=2,
        data_dir=tmp_path,
    )

    assert result["top_n"] == 2
    assert "score_bucket_report" in result
    assert "top_n_report" in result
    assert "execution_report" in result
