import json

import pandas as pd

from hermes.research.continuation_study import run_continuation_study


def test_run_continuation_study_builds_top_n_hold_days_grid(tmp_path):
    ranked = pd.DataFrame(
        [
            {
                "trade_date": "2026-01-02",
                "code": "600036",
                "rank": 1,
                "score": 5.8,
                "t1_return": 0.02,
                "t2_return": 0.03,
                "t3_return": 0.01,
                "open_t1_return": 0.01,
                "vwap_30m_t1_return": 0.015,
                "open_not_chase_t1_return": 0.009,
            },
            {
                "trade_date": "2026-01-02",
                "code": "000001",
                "rank": 2,
                "score": 4.9,
                "t1_return": -0.02,
                "t2_return": -0.03,
                "t3_return": -0.01,
                "open_t1_return": -0.01,
                "vwap_30m_t1_return": -0.008,
                "open_not_chase_t1_return": -0.012,
            },
            {
                "trade_date": "2026-01-03",
                "code": "600519",
                "rank": 1,
                "score": 6.1,
                "t1_return": 0.01,
                "t2_return": 0.06,
                "t3_return": 0.04,
                "open_t1_return": 0.02,
                "vwap_30m_t1_return": 0.017,
                "open_not_chase_t1_return": 0.016,
            },
        ]
    )
    forward = pd.DataFrame(
        [
            {"trade_date": "2026-01-02", "code": "600036", "t1_return": 0.02, "t2_return": 0.03, "t3_return": 0.01},
            {"trade_date": "2026-01-02", "code": "000001", "t1_return": -0.02, "t2_return": -0.03, "t3_return": -0.01},
            {"trade_date": "2026-01-03", "code": "600519", "t1_return": 0.01, "t2_return": 0.06, "t3_return": 0.04},
        ]
    )
    results = [
        {
            "trade_date": "2026-01-02",
            "code": "600036",
            "name": "招商银行",
            "qualified": True,
            "strength_score": 2.0,
            "continuity_score": 1.1,
            "quality_score": 1.2,
            "flow_score": 0.6,
            "stability_score": 0.9,
            "overheat_penalty": 0.0,
            "notes": [],
        },
        {
            "trade_date": "2026-01-02",
            "code": "000001",
            "name": "平安银行",
            "qualified": True,
            "strength_score": 1.7,
            "continuity_score": 0.9,
            "quality_score": 1.0,
            "flow_score": 0.5,
            "stability_score": 0.8,
            "overheat_penalty": 0.0,
            "notes": [],
        },
        {
            "trade_date": "2026-01-03",
            "code": "600519",
            "name": "贵州茅台",
            "qualified": True,
            "strength_score": 2.2,
            "continuity_score": 1.2,
            "quality_score": 1.3,
            "flow_score": 0.7,
            "stability_score": 0.7,
            "overheat_penalty": 0.0,
            "notes": [],
        },
    ]

    ranked.to_csv(tmp_path / "ranked_returns.csv", index=False)
    forward.to_csv(tmp_path / "forward_returns.csv", index=False)
    (tmp_path / "results.json").write_text(json.dumps(results), encoding="utf-8")

    result = run_continuation_study(
        codes=["600036", "000001", "600519"],
        start="2026-01-01",
        end="2026-01-31",
        top_ns=(1, 2),
        hold_days_list=(1, 2),
        data_dir=tmp_path,
    )

    assert len(result["comparison_report"]) == 4
    assert result["best_setup"]["top_n"] == 1
    assert result["best_setup"]["hold_days"] == 2
    assert result["best_setup"]["total_return_pct"] == 9.0
    assert result["comparison_report"][0]["trade_count"] >= 1
    assert result["validation_snapshot"]["top_n"] == 2

