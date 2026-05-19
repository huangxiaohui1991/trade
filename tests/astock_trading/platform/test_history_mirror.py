"""历史信号镜像归档与诊断。"""

from __future__ import annotations

from astock_trading.platform.db import connect, init_db
from astock_trading.platform.history_mirror import (
    archive_signal_history,
    diagnose_signal_history,
)


def test_archive_and_diagnose_signal_history_bundle(tmp_path):
    db_path = tmp_path / "history.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        group_id = archive_signal_history(
            conn,
            snapshot_date="2026-05-19",
            history_group_id="hist_20260519_1",
            run_id="screener_101500",
            phase="screener",
            market={
                "signal": "YELLOW",
                "indices": {"上证指数": {"change_pct": 0.2}},
            },
            pool=[
                {"code": "002138", "name": "双环传动", "pool_tier": "watch", "score": 5.8},
            ],
            candidates=[
                {
                    "code": "002138",
                    "name": "双环传动",
                    "total_score": 5.8,
                    "entry_signal": False,
                    "data_quality": "ok",
                    "hard_veto_signals": [],
                },
            ],
            decisions=[
                {
                    "code": "002138",
                    "name": "双环传动",
                    "action": "WATCH",
                    "score": 5.8,
                    "notes": ["缺少入场信号"],
                },
            ],
        )

        payload = diagnose_signal_history(
            conn,
            snapshot_date="2026-05-19",
            history_group_id=group_id,
            code="002138",
        )
    finally:
        conn.close()

    assert group_id == "hist_20260519_1"
    assert payload["status"] == "ok"
    assert payload["snapshot_date"] == "2026-05-19"
    assert payload["history_group_id"] == "hist_20260519_1"
    assert payload["sections"]["market"]["signal"] == "YELLOW"
    assert payload["sections"]["pool"][0]["code"] == "002138"
    assert payload["sections"]["candidates"][0]["total_score"] == 5.8
    assert payload["sections"]["decision"][0]["action"] == "WATCH"
    assert payload["code_analysis"]["code"] == "002138"
    assert payload["code_analysis"]["decision_action"] == "WATCH"
    assert "观察" in payload["code_analysis"]["miss_reason"]
