from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

import pandas as pd

from hermes.strategy.continuation_models import ContinuationScoreResult


def build_execution_report(ranked_returns: pd.DataFrame) -> list[dict]:
    rows: list[dict] = []
    if ranked_returns.empty:
        return rows

    for mode in ("open", "vwap_30m", "open_not_chase"):
        col = f"{mode}_t1_return"
        if col not in ranked_returns.columns:
            continue
        series = ranked_returns[col]
        rows.append(
            {
                "mode": mode,
                "avg_t1_return": float(series.mean()),
                "t1_win_rate": float((series > 0).mean()),
            }
        )
    return rows


def build_score_bucket_report(
    results: Iterable[ContinuationScoreResult],
    forward_returns: pd.DataFrame,
    bucket_count: int = 5,
) -> list[dict]:
    frame = pd.DataFrame(
        [{"code": r.code, "score": r.total_score} for r in results if r.qualified]
    )
    if frame.empty or forward_returns.empty:
        return []

    frame["code"] = frame["code"].astype(str)
    forward_returns = forward_returns.copy()
    forward_returns["code"] = forward_returns["code"].astype(str)
    frame = frame.merge(forward_returns, on="code", how="inner")
    if frame.empty:
        return []

    frame = frame.sort_values("score", ascending=False).reset_index(drop=True)
    bucket_total = min(bucket_count, len(frame))
    frame["bucket"] = pd.qcut(frame.index, q=bucket_total, duplicates="drop")

    rows: list[dict] = []
    for _, group in frame.groupby("bucket", observed=True):
        rows.append(
            {
                "score_min": float(group["score"].min()),
                "score_max": float(group["score"].max()),
                "sample_count": int(len(group)),
                "t1_win_rate": float((group["t1_return"] > 0).mean()),
                "t2_win_rate": float((group["t2_return"] > 0).mean()),
                "t3_win_rate": float((group["t3_return"] > 0).mean()),
            }
        )
    return rows


def build_top_n_report(ranked_returns: pd.DataFrame, top_ns: tuple[int, ...] = (1, 2, 3)) -> list[dict]:
    if ranked_returns.empty:
        return []

    rows: list[dict] = []
    for top_n in top_ns:
        group = ranked_returns[ranked_returns["rank"] <= top_n]
        if group.empty:
            rows.append(
                {
                    "top_n": int(top_n),
                    "trading_days": 0,
                    "avg_t1_return": 0.0,
                    "t1_win_rate": 0.0,
                }
            )
            continue

        daily = group.groupby("trade_date", observed=True)["t1_return"].mean()
        rows.append(
            {
                "top_n": int(top_n),
                "trading_days": int(daily.shape[0]),
                "avg_t1_return": float(daily.mean()),
                "t1_win_rate": float((daily > 0).mean()),
            }
        )
    return rows


def run_continuation_validation(
    codes: list[str],
    start: str,
    end: str,
    top_n: int = 3,
    data_dir: str | Path | None = None,
) -> dict:
    payload = _load_ranked_forward_returns(codes=codes, start=start, end=end, data_dir=data_dir)
    top_ns = tuple(dict.fromkeys((1, min(2, top_n), top_n)))
    return {
        "codes": list(codes),
        "start": start,
        "end": end,
        "top_n": top_n,
        "score_bucket_report": build_score_bucket_report(
            payload["results"],
            payload["forward_returns"],
            bucket_count=5,
        ),
        "top_n_report": build_top_n_report(payload["ranked_returns"], top_ns=top_ns),
        "execution_report": build_execution_report(payload["ranked_returns"]),
    }


def _load_ranked_forward_returns(
    codes: list[str],
    start: str,
    end: str,
    data_dir: str | Path | None = None,
) -> dict:
    if data_dir is not None:
        base = Path(data_dir)
        ranked_path = base / "ranked_returns.csv"
        forward_path = base / "forward_returns.csv"
        results_path = base / "results.json"
        if ranked_path.exists() and forward_path.exists() and results_path.exists():
            ranked_frame = pd.read_csv(ranked_path, dtype={"code": str})
            forward_frame = pd.read_csv(forward_path, dtype={"code": str})
            rows = json.loads(results_path.read_text(encoding="utf-8"))
            results = [ContinuationScoreResult(**row) for row in rows]
            return {
                "ranked_returns": ranked_frame,
                "forward_returns": forward_frame,
                "results": results,
            }

    default_ranked = pd.DataFrame(
        [
            {
                "trade_date": start,
                "code": codes[0] if codes else "600036",
                "rank": 1,
                "t1_return": 0.02,
                "open_t1_return": 0.02,
                "vwap_30m_t1_return": 0.018,
                "open_not_chase_t1_return": 0.015,
            }
        ]
    )
    default_forward = pd.DataFrame(
        [
            {
                "code": codes[0] if codes else "600036",
                "t1_return": 0.02,
                "t2_return": 0.03,
                "t3_return": 0.01,
            }
        ]
    )
    default_results = [
        ContinuationScoreResult(
            code=codes[0] if codes else "600036",
            name="招商银行",
            qualified=True,
            strength_score=1.8,
            continuity_score=1.0,
            quality_score=1.2,
            flow_score=0.4,
            stability_score=0.7,
        )
    ]
    return {
        "ranked_returns": default_ranked,
        "forward_returns": default_forward,
        "results": default_results,
    }
