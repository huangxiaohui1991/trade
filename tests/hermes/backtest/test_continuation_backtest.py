import json

from hermes.backtest.continuation_backtest import run_continuation_backtest


def test_continuation_backtest_returns_hold_window_metrics(tmp_path):
    trades = [
        {
            "trade_date": "2026-01-02",
            "code": "600036",
            "hold_days": 2,
            "return_pct": 1.8,
            "entry_mode": "open",
            "top_n": 2,
        },
        {
            "trade_date": "2026-01-03",
            "code": "000001",
            "hold_days": 2,
            "return_pct": -0.4,
            "entry_mode": "open",
            "top_n": 2,
        },
    ]
    (tmp_path / "continuation_trades.json").write_text(json.dumps(trades), encoding="utf-8")

    result = run_continuation_backtest(
        codes=["600036", "000001"],
        start="2026-01-01",
        end="2026-03-31",
        hold_days=2,
        top_n=2,
        data_dir=tmp_path,
    )

    assert result["hold_days"] == 2
    assert "total_return_pct" in result
    assert "win_rate_pct" in result
    assert "trades" in result
    assert len(result["trades"]) == 2
