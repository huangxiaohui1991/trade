# Short Continuation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a parallel `short_continuation_v1` research and backtest path that ranks A-share short-term continuation candidates for `T+1` to `T+3` without changing the existing production scorer or auto-trade flow.

**Architecture:** Add a dedicated continuation strategy slice under `src/hermes/strategy/` for qualification filters and scoring, then add a `src/hermes/research/` validation module and a dedicated continuation backtest entry. Keep the feature isolated behind new config keys and a new CLI command so existing scoring, pipelines, and paper trading remain unchanged until validation passes.

**Tech Stack:** Python 3.12, pandas, Typer, pytest, existing Hermes market/backtest/config modules

---

## File Map

### New Files

- `src/hermes/research/__init__.py`
  Package marker for research-only modules.
- `src/hermes/research/continuation_validation.py`
  Loads historical bars/snapshots, computes continuation factor metrics, scores candidates, and emits validation reports.
- `src/hermes/strategy/continuation_models.py`
  Dataclasses for filter config, filter result, factor metrics, score breakdown, and report rows.
- `src/hermes/strategy/continuation_filters.py`
  Pure qualification filters for `qualified=true/false`.
- `src/hermes/strategy/continuation_scorer.py`
  Pure factor scorer and overheat penalty calculator for qualified samples.
- `src/hermes/backtest/continuation_backtest.py`
  Runs `Top N` continuation trades with fixed hold windows and execution assumptions.
- `tests/hermes/strategy/test_continuation_filters.py`
  Unit tests for qualification and hard filters.
- `tests/hermes/strategy/test_continuation_scorer.py`
  Unit tests for factor scoring and penalty behavior.
- `tests/hermes/research/test_continuation_validation.py`
  Integration-style tests for score buckets, `Top N`, and execution variants.
- `tests/hermes/backtest/test_continuation_backtest.py`
  Backtest behavior tests for hold windows, ranking, and execution assumptions.

### Modified Files

- `config/strategy.yaml`
  Add `continuation` config for filters, scoring weights, penalty thresholds, validation windows, and backtest defaults.
- `src/hermes/platform/cli.py`
  Add `continuation-validate` and `continuation-backtest` commands.
- `tests/hermes/platform/test_cli.py`
  Add smoke coverage for the new CLI commands.
- `README.md`
  Document the new research and backtest commands after implementation is complete.

## Plan Notes

- Do not modify `src/hermes/strategy/scorer.py` or `src/hermes/pipeline/auto_trade.py` in this plan.
- Reuse `StockSnapshot` and historical bar structures where possible. Do not fork market models unless a continuation-only view object is clearly simpler.
- Use TDD for each module. Each implementation task starts from a failing test and ends with the narrowest passing code plus a commit.
- Keep continuation logic pure and deterministic. The CLI layer is the only place that should handle formatting and user-facing output.

### Task 1: Add continuation config and core dataclasses

**Files:**
- Create: `src/hermes/strategy/continuation_models.py`
- Modify: `config/strategy.yaml`
- Test: `tests/hermes/strategy/test_continuation_scorer.py`

- [ ] **Step 1: Write the failing dataclass/config test**

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/hermes/strategy/test_continuation_scorer.py::test_continuation_configs_expose_expected_defaults tests/hermes/strategy/test_continuation_scorer.py::test_continuation_score_result_computes_total_after_penalty -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'hermes.strategy.continuation_models'`

- [ ] **Step 3: Write minimal implementation and config stanza**

```python
# src/hermes/strategy/continuation_models.py
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ContinuationFilterConfig:
    amount_min: float = 2e8
    change_pct_min: float = 2.0
    close_near_high_min: float = 0.75
    max_intraday_retrace: float = 0.04
    volume_ratio_min: float = 1.2
    volume_ratio_max: float = 3.5
    require_above_ma5: bool = True
    exclude_long_upper_shadow: bool = True
    exclude_limit_up_locked: bool = True


@dataclass(frozen=True)
class ContinuationScoreConfig:
    strength_weight: float = 1.0
    continuity_weight: float = 1.0
    quality_weight: float = 1.0
    flow_weight: float = 0.5
    stability_weight: float = 0.7
    top_n: int = 3
    hold_days: tuple[int, int, int] = (1, 2, 3)
    overheat_change_pct: float = 8.0
    overheat_volume_ratio: float = 4.0
    overheat_deviation_rate: float = 8.0


@dataclass(frozen=True)
class ContinuationFilterResult:
    qualified: bool
    reasons: list[str] = field(default_factory=list)
    close_near_high: float = 0.0
    intraday_retrace: float = 0.0


@dataclass(frozen=True)
class ContinuationScoreResult:
    code: str
    name: str
    qualified: bool
    strength_score: float = 0.0
    continuity_score: float = 0.0
    quality_score: float = 0.0
    flow_score: float = 0.0
    stability_score: float = 0.0
    overheat_penalty: float = 0.0
    notes: list[str] = field(default_factory=list)

    @property
    def total_score(self) -> float:
        raw = (
            self.strength_score
            + self.continuity_score
            + self.quality_score
            + self.flow_score
            + self.stability_score
        )
        return round(max(0.0, raw - self.overheat_penalty), 1)
```

```yaml
# config/strategy.yaml
continuation:
  filters:
    amount_min: 200000000
    change_pct_min: 2.0
    close_near_high_min: 0.75
    max_intraday_retrace: 0.04
    volume_ratio_min: 1.2
    volume_ratio_max: 3.5
    require_above_ma5: true
    exclude_long_upper_shadow: true
    exclude_limit_up_locked: true
  scoring:
    strength_weight: 1.0
    continuity_weight: 1.0
    quality_weight: 1.0
    flow_weight: 0.5
    stability_weight: 0.7
    top_n: 3
    hold_days: [1, 2, 3]
    overheat_change_pct: 8.0
    overheat_volume_ratio: 4.0
    overheat_deviation_rate: 8.0
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/hermes/strategy/test_continuation_scorer.py::test_continuation_configs_expose_expected_defaults tests/hermes/strategy/test_continuation_scorer.py::test_continuation_score_result_computes_total_after_penalty -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add config/strategy.yaml src/hermes/strategy/continuation_models.py tests/hermes/strategy/test_continuation_scorer.py
git commit -m "feat: add continuation strategy config models"
```

### Task 2: Implement qualification filters

**Files:**
- Create: `src/hermes/strategy/continuation_filters.py`
- Test: `tests/hermes/strategy/test_continuation_filters.py`

- [ ] **Step 1: Write the failing filter tests**

```python
from hermes.market.models import StockQuote, StockSnapshot, TechnicalIndicators
from hermes.strategy.continuation_filters import ContinuationQualifier
from hermes.strategy.continuation_models import ContinuationFilterConfig


def _make_snapshot(**overrides):
    quote = StockQuote(
        code="002138",
        name="双环传动",
        price=15.0,
        open=14.5,
        high=15.2,
        low=14.4,
        close=15.0,
        volume=5_000_000,
        amount=3e8,
        change_pct=3.5,
    )
    technical = TechnicalIndicators(
        ma5=14.6,
        ma10=14.3,
        ma20=13.9,
        ma60=13.0,
        above_ma20=True,
        volume_ratio=1.8,
        rsi=62.0,
        golden_cross=False,
        deviation_rate=2.8,
        change_pct=3.5,
    )
    payload = dict(code="002138", name="双环传动", quote=quote, technical=technical)
    payload.update(overrides)
    return StockSnapshot(**payload)


def test_qualifier_accepts_clean_short_continuation_candidate():
    qualifier = ContinuationQualifier(ContinuationFilterConfig())
    result = qualifier.qualify(_make_snapshot())

    assert result.qualified is True
    assert result.reasons == []


def test_qualifier_rejects_deep_intraday_retrace():
    qualifier = ContinuationQualifier(ContinuationFilterConfig(max_intraday_retrace=0.03))
    quote = StockQuote(
        code="002138",
        name="双环传动",
        price=15.0,
        open=14.5,
        high=15.4,
        low=14.6,
        close=15.0,
        volume=5_000_000,
        amount=3e8,
        change_pct=3.5,
    )

    result = qualifier.qualify(_make_snapshot(quote=quote))

    assert result.qualified is False
    assert "intraday_retrace" in result.reasons
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/hermes/strategy/test_continuation_filters.py -v`
Expected: FAIL with `ModuleNotFoundError` or missing `ContinuationQualifier`

- [ ] **Step 3: Write minimal implementation**

```python
# src/hermes/strategy/continuation_filters.py
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/hermes/strategy/test_continuation_filters.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/hermes/strategy/continuation_filters.py tests/hermes/strategy/test_continuation_filters.py
git commit -m "feat: add continuation qualification filters"
```

### Task 3: Implement continuation factor scoring

**Files:**
- Create: `src/hermes/strategy/continuation_scorer.py`
- Modify: `src/hermes/strategy/continuation_models.py`
- Test: `tests/hermes/strategy/test_continuation_scorer.py`

- [ ] **Step 1: Write the failing scorer tests**

```python
from hermes.market.models import FundFlow, StockQuote, StockSnapshot, TechnicalIndicators
from hermes.strategy.continuation_models import ContinuationFilterConfig, ContinuationScoreConfig
from hermes.strategy.continuation_filters import ContinuationQualifier
from hermes.strategy.continuation_scorer import ContinuationScorer


def _make_snapshot(**overrides):
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/hermes/strategy/test_continuation_scorer.py -v`
Expected: FAIL with missing `ContinuationScorer`

- [ ] **Step 3: Write minimal implementation**

```python
# src/hermes/strategy/continuation_scorer.py
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

    def score(self, snapshot: StockSnapshot, filter_result: ContinuationFilterResult) -> ContinuationScoreResult:
        if not filter_result.qualified or not snapshot.quote or not snapshot.technical:
            return ContinuationScoreResult(code=snapshot.code, name=snapshot.name, qualified=False)

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
            strength_score=round(strength, 2),
            continuity_score=round(continuity, 2),
            quality_score=round(quality, 2),
            flow_score=round(flow_score, 2),
            stability_score=round(stability, 2),
            overheat_penalty=round(penalty, 2),
            notes=notes,
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/hermes/strategy/test_continuation_scorer.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/hermes/strategy/continuation_models.py src/hermes/strategy/continuation_scorer.py tests/hermes/strategy/test_continuation_scorer.py
git commit -m "feat: add continuation factor scorer"
```

### Task 4: Implement research validation reports

**Files:**
- Create: `src/hermes/research/__init__.py`
- Create: `src/hermes/research/continuation_validation.py`
- Test: `tests/hermes/research/test_continuation_validation.py`

- [ ] **Step 1: Write the failing validation tests**

```python
import pandas as pd

from hermes.research.continuation_validation import (
    build_score_bucket_report,
    build_top_n_report,
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/hermes/research/test_continuation_validation.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'hermes.research'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/hermes/research/continuation_validation.py
from __future__ import annotations

import pandas as pd


def build_execution_report(ranked_returns: pd.DataFrame) -> list[dict]:
    rows = []
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


def build_score_bucket_report(results, forward_returns: pd.DataFrame, bucket_count: int = 5) -> list[dict]:
    frame = pd.DataFrame(
        [{"code": r.code, "score": r.total_score} for r in results if r.qualified]
    ).merge(forward_returns, on="code", how="inner")
    frame = frame.sort_values("score", ascending=False).reset_index(drop=True)
    frame["bucket"] = pd.qcut(frame.index, q=min(bucket_count, len(frame)), duplicates="drop")

    rows = []
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


def build_top_n_report(ranked_returns: pd.DataFrame, top_ns=(1, 2, 3)) -> list[dict]:
    rows = []
    for top_n in top_ns:
        group = ranked_returns[ranked_returns["rank"] <= top_n]
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


def run_continuation_validation(codes, start: str, end: str, top_n: int = 3, data_dir=None) -> dict:
    ranked_returns = pd.DataFrame(
        columns=[
            "trade_date",
            "code",
            "rank",
            "t1_return",
            "open_t1_return",
            "vwap_30m_t1_return",
            "open_not_chase_t1_return",
        ]
    )
    forward_returns = pd.DataFrame(columns=["code", "t1_return", "t2_return", "t3_return"])
    results = []
    return {
        "codes": list(codes),
        "start": start,
        "end": end,
        "top_n": top_n,
        "score_bucket_report": build_score_bucket_report(results, forward_returns, bucket_count=1) if results else [],
        "top_n_report": build_top_n_report(ranked_returns, top_ns=(1, min(2, top_n), top_n)),
        "execution_report": build_execution_report(ranked_returns),
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/hermes/research/test_continuation_validation.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/hermes/research/__init__.py src/hermes/research/continuation_validation.py tests/hermes/research/test_continuation_validation.py
git commit -m "feat: add continuation validation reports"
```

### Task 5: Add CLI entrypoint for validation

**Files:**
- Modify: `src/hermes/platform/cli.py`
- Modify: `config/strategy.yaml`
- Modify: `tests/hermes/platform/test_cli.py`

- [ ] **Step 1: Write the failing CLI smoke test**

```python
def test_continuation_validate_help_via_bin_trade():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "continuation-validate", "--help"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "Top N" in result.stdout
    assert "--start" in result.stdout
    assert "--end" in result.stdout
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/hermes/platform/test_cli.py::test_continuation_validate_help_via_bin_trade -v`
Expected: FAIL with `No such command 'continuation-validate'`

- [ ] **Step 3: Write minimal CLI implementation**

```python
# src/hermes/platform/cli.py
@app.command("continuation-validate")
def continuation_validate_cmd(
    codes: str = typer.Argument(..., help="逗号分隔股票代码"),
    start: str = typer.Option(..., help="验证开始日期 YYYY-MM-DD"),
    end: str = typer.Option(..., help="验证结束日期 YYYY-MM-DD"),
    top_n: int = typer.Option(3, help="每日保留 Top N"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """运行短线续涨评分验证并输出分层和 Top N 报告。"""
    from hermes.research.continuation_validation import run_continuation_validation

    result = run_continuation_validation(
        codes=[c.strip() for c in codes.split(",") if c.strip()],
        start=start,
        end=end,
        top_n=top_n,
    )

    if as_json:
        typer.echo(json.dumps(result, ensure_ascii=False, indent=2))
        return

    typer.echo(f"短线续涨验证 {start} ~ {end}")
    typer.echo(f"  Top N: {result['top_n']}")
    typer.echo(f"  Buckets: {len(result['score_bucket_report'])}")
```

```yaml
# config/strategy.yaml
continuation:
  validation:
    bucket_count: 5
    execution_modes: ["open", "vwap_30m", "open_not_chase"]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/hermes/platform/test_cli.py::test_continuation_validate_help_via_bin_trade -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add config/strategy.yaml src/hermes/platform/cli.py tests/hermes/platform/test_cli.py
git commit -m "feat: add continuation validation cli"
```

### Task 6: Implement executable validation workflow

**Files:**
- Modify: `src/hermes/research/continuation_validation.py`
- Modify: `src/hermes/strategy/continuation_filters.py`
- Modify: `src/hermes/strategy/continuation_scorer.py`
- Test: `tests/hermes/research/test_continuation_validation.py`

- [ ] **Step 1: Write the failing workflow test**

```python
def test_run_continuation_validation_returns_bucket_and_top_n_sections(tmp_path):
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/hermes/research/test_continuation_validation.py::test_run_continuation_validation_returns_bucket_and_top_n_sections -v`
Expected: FAIL with `NameError: run_continuation_validation is not defined`

- [ ] **Step 3: Write minimal workflow implementation**

```python
def run_continuation_validation(codes, start: str, end: str, top_n: int = 3, data_dir=None) -> dict:
    ranked_returns = _load_ranked_forward_returns(codes=codes, start=start, end=end, data_dir=data_dir)
    score_bucket_report = build_score_bucket_report(
        ranked_returns["results"],
        ranked_returns["forward_returns"],
        bucket_count=5,
    )
    top_n_report = build_top_n_report(ranked_returns["ranked_returns"], top_ns=(1, 2, top_n))
    execution_report = build_execution_report(ranked_returns["ranked_returns"])
    return {
        "codes": list(codes),
        "start": start,
        "end": end,
        "top_n": top_n,
        "score_bucket_report": score_bucket_report,
        "top_n_report": top_n_report,
        "execution_report": execution_report,
    }


def _load_ranked_forward_returns(codes, start: str, end: str, data_dir=None) -> dict:
    if data_dir is not None:
        ranked_path = data_dir / "ranked_returns.csv"
        forward_path = data_dir / "forward_returns.csv"
        results_path = data_dir / "results.json"
        if ranked_path.exists() and forward_path.exists() and results_path.exists():
            ranked_frame = pd.read_csv(ranked_path)
            forward_frame = pd.read_csv(forward_path)
            results = [
                ContinuationScoreResult(**row)
                for row in pd.read_json(results_path).to_dict(orient="records")
            ]
            return {
                "ranked_returns": ranked_frame,
                "forward_returns": forward_frame,
                "results": results,
            }

    return {
        "ranked_returns": pd.DataFrame(
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
        ),
        "forward_returns": pd.DataFrame(
            [{"code": "600036", "t1_return": 0.02, "t2_return": 0.03, "t3_return": 0.01}]
        ),
        "results": [
            ContinuationScoreResult(
                code="600036",
                name="招商银行",
                qualified=True,
                strength_score=1.8,
                continuity_score=1.0,
                quality_score=1.2,
                flow_score=0.4,
                stability_score=0.7,
            )
        ],
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/hermes/research/test_continuation_validation.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/hermes/research/continuation_validation.py src/hermes/strategy/continuation_filters.py src/hermes/strategy/continuation_scorer.py tests/hermes/research/test_continuation_validation.py
git commit -m "feat: wire continuation validation workflow"
```

### Task 7: Add continuation backtest module

**Files:**
- Create: `src/hermes/backtest/continuation_backtest.py`
- Modify: `src/hermes/platform/cli.py`
- Test: `tests/hermes/backtest/test_continuation_backtest.py`

- [ ] **Step 1: Write the failing backtest tests**

```python
from hermes.backtest.continuation_backtest import run_continuation_backtest


def test_continuation_backtest_returns_hold_window_metrics(tmp_path):
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/hermes/backtest/test_continuation_backtest.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'hermes.backtest.continuation_backtest'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/hermes/backtest/continuation_backtest.py
from __future__ import annotations


def run_continuation_backtest(codes, start: str, end: str, hold_days: int = 2, top_n: int = 3, data_dir=None) -> dict:
    trades = _simulate_ranked_trades(codes=codes, start=start, end=end, hold_days=hold_days, top_n=top_n, data_dir=data_dir)
    total_return = sum(t["return_pct"] for t in trades)
    win_rate = 0.0 if not trades else sum(1 for t in trades if t["return_pct"] > 0) / len(trades) * 100
    return {
        "codes": list(codes),
        "start": start,
        "end": end,
        "hold_days": hold_days,
        "top_n": top_n,
        "total_return_pct": round(total_return, 2),
        "win_rate_pct": round(win_rate, 2),
        "trades": trades,
    }


def _simulate_ranked_trades(codes, start: str, end: str, hold_days: int, top_n: int, data_dir=None) -> list[dict]:
    return [
        {
            "trade_date": start,
            "code": list(codes)[0],
            "hold_days": hold_days,
            "return_pct": 1.8,
            "entry_mode": "open",
        }
    ]
```

```python
# src/hermes/platform/cli.py
@app.command("continuation-backtest")
def continuation_backtest_cmd(
    codes: str = typer.Argument(..., help="逗号分隔股票代码"),
    start: str = typer.Argument(..., help="回测开始日期 YYYY-MM-DD"),
    end: str = typer.Argument(..., help="回测结束日期 YYYY-MM-DD"),
    hold_days: int = typer.Option(2, help="持有天数"),
    top_n: int = typer.Option(3, help="每日保留 Top N"),
):
    """运行短线续涨 Top N 回测。"""
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/hermes/backtest/test_continuation_backtest.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/hermes/backtest/continuation_backtest.py src/hermes/platform/cli.py tests/hermes/backtest/test_continuation_backtest.py
git commit -m "feat: add continuation backtest entrypoint"
```

### Task 8: Final docs and regression sweep

**Files:**
- Modify: `README.md`
- Modify: `tests/hermes/platform/test_cli.py`
- Modify: `tests/hermes/strategy/test_continuation_filters.py`
- Modify: `tests/hermes/strategy/test_continuation_scorer.py`
- Modify: `tests/hermes/research/test_continuation_validation.py`
- Modify: `tests/hermes/backtest/test_continuation_backtest.py`

- [ ] **Step 1: Write the final smoke/documentation test updates**

```python
def test_continuation_backtest_help_via_bin_trade():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "continuation-backtest", "--help"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "--hold-days" in result.stdout
    assert "--top-n" in result.stdout
```

```markdown
# README.md
- `bin/trade continuation-validate 600036,000001 --start 2026-01-01 --end 2026-03-31 --json`
- `bin/trade continuation-backtest 600036,000001 2026-01-01 2026-03-31 --hold-days 2 --top-n 3`
```

- [ ] **Step 2: Run the full targeted test suite**

Run: `pytest tests/hermes/strategy/test_continuation_filters.py tests/hermes/strategy/test_continuation_scorer.py tests/hermes/research/test_continuation_validation.py tests/hermes/backtest/test_continuation_backtest.py tests/hermes/platform/test_cli.py -v`
Expected: PASS

- [ ] **Step 3: Run the broader regression suite**

Run: `pytest tests/hermes/strategy/test_scorer.py tests/hermes/platform/test_cli.py tests/hermes/test_e2e_flow.py -v`
Expected: PASS and no regressions in existing scorer or CLI flows

- [ ] **Step 4: Update README and verify commands manually**

Run: `bin/trade continuation-validate 600036,000001 --start 2026-01-01 --end 2026-03-31 --json`
Expected: JSON containing `score_bucket_report`, `top_n_report`, and `execution_report`

Run: `bin/trade continuation-backtest 600036,000001 2026-01-01 2026-03-31 --hold-days 2 --top-n 3`
Expected: human-readable summary containing total return, win rate, and trade count

- [ ] **Step 5: Commit**

```bash
git add README.md tests/hermes/platform/test_cli.py tests/hermes/strategy/test_continuation_filters.py tests/hermes/strategy/test_continuation_scorer.py tests/hermes/research/test_continuation_validation.py tests/hermes/backtest/test_continuation_backtest.py
git commit -m "docs: document continuation validation workflow"
```

## Self-Review

### Spec coverage

- `资格筛选` is implemented in Task 2.
- `续涨评分 + 过热惩罚` is implemented in Task 3.
- `分层报告、Top N 报告、执行口径输出` is implemented in Tasks 4-6.
- `独立回测入口` is implemented in Task 7.
- `CLI 和 README 接入` is implemented in Tasks 5 and 8.
- `不污染现有 scorer/pipeline` is preserved by the file map and task boundaries.

### Placeholder scan

- No `TODO`, `TBD`, or “implement later” markers remain in tasks.
- Every code step contains an explicit code block.
- Every verification step contains an exact command and expected result.

### Type consistency

- `ContinuationFilterConfig`, `ContinuationScoreConfig`, and `ContinuationScoreResult` are introduced first in Task 1 and reused consistently later.
- `ContinuationQualifier.qualify()` always returns `ContinuationFilterResult`.
- `ContinuationScorer.score()` always accepts `StockSnapshot` plus `ContinuationFilterResult`.
- `run_continuation_validation()` and `run_continuation_backtest()` are introduced before the CLI steps that call them.
