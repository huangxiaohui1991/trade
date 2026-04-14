"""Tests for strategy/service.py and risk/service.py — service layer with event_log"""

import pytest
from datetime import date

from hermes.platform.db import init_db, connect
from hermes.platform.events import EventStore
from hermes.market.models import (
    FinancialReport,
    FundFlow,
    SentimentData,
    StockQuote,
    StockSnapshot,
    TechnicalIndicators,
)
from hermes.strategy.models import (
    Action,
    MarketSignal,
    MarketState,
    ScoringWeights,
)
from hermes.strategy.scorer import Scorer
from hermes.strategy.decider import Decider
from hermes.strategy.service import StrategyService
from hermes.risk.models import PortfolioLimits, RiskParams
from hermes.risk.service import RiskService
from hermes.strategy.models import Style


@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)
    yield conn
    conn.close()


@pytest.fixture
def event_store(db):
    return EventStore(db)


def _make_snapshot(code="002138", name="双环传动") -> StockSnapshot:
    return StockSnapshot(
        code=code, name=name,
        quote=StockQuote(
            code=code, name=name, price=15.0,
            open=14.8, high=15.2, low=14.7, close=15.0,
            volume=5000000, amount=7.5e8, change_pct=1.5,
        ),
        technical=TechnicalIndicators(
            ma5=15.0, ma10=14.5, ma20=14.0, ma60=13.0,
            above_ma20=True, volume_ratio=1.8, rsi=55.0,
            golden_cross=True, ma20_slope=0.01,
            momentum_5d=3.0, daily_volatility=0.025,
        ),
        financial=FinancialReport(roe=12.0, revenue_growth=15.0, operating_cash_flow=1e8),
        flow=FundFlow(net_inflow_1d=6e8, northbound_net_positive=True),
        sentiment=SentimentData(score=2.0, detail="研报3篇"),
    )


class TestStrategyService:
    def test_evaluate_writes_events(self, event_store):
        scorer = Scorer(
            weights=ScoringWeights(technical=3, fundamental=2, flow=2, sentiment=3),
            veto_rules=["below_ma20"],
        )
        decider = Decider(buy_threshold=6.5, watch_threshold=5.0)
        svc = StrategyService(scorer, decider, event_store)

        snapshots = [_make_snapshot("001", "股票A"), _make_snapshot("002", "股票B")]
        market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)

        decisions = svc.evaluate(
            snapshots, market,
            run_id="run_test_001", config_version="v_test",
        )

        assert len(decisions) == 2

        # 验证 score.calculated 事件写入
        score_events = event_store.query(event_type="score.calculated")
        assert len(score_events) == 2

        # 验证 decision.suggested 事件写入
        decision_events = event_store.query(event_type="decision.suggested")
        assert len(decision_events) == 2

        # 验证 metadata
        for ev in score_events:
            assert ev["metadata"]["run_id"] == "run_test_001"
            assert ev["metadata"]["config_version"] == "v_test"

    def test_score_single_writes_event(self, event_store):
        scorer = Scorer(
            weights=ScoringWeights(technical=3, fundamental=2, flow=2, sentiment=3),
            veto_rules=[],
        )
        decider = Decider()
        svc = StrategyService(scorer, decider, event_store)

        result = svc.score_single(
            _make_snapshot(), run_id="run_single", config_version="v1",
        )

        assert result.code == "002138"
        events = event_store.query(event_type="score.calculated")
        assert len(events) == 1
        assert events[0]["payload"]["code"] == "002138"


class TestRiskService:
    def test_assess_position_writes_events(self, event_store):
        svc = RiskService(event_store)

        signals = svc.assess_position(
            code="002138",
            avg_cost=50.0,
            current_price=45.0,
            entry_date=date(2026, 4, 1),
            today=date(2026, 4, 10),
            highest_since_entry=52.0,
            entry_day_low=49.0,
            risk_params=RiskParams(style=Style.MOMENTUM, stop_loss=0.08),
            run_id="run_risk_001",
        )

        assert len(signals) >= 1
        risk_events = event_store.query(stream="risk:002138")
        assert len(risk_events) >= 1

    def test_assess_portfolio_writes_events(self, event_store):
        svc = RiskService(event_store)

        breaches = svc.assess_portfolio(
            daily_pnl_pct=-0.04,
            consecutive_loss_days=1,
            max_single_exposure_pct=0.15,
            max_sector_exposure_pct=0.30,
            limits=PortfolioLimits(daily_loss_limit_pct=0.03),
            run_id="run_risk_002",
        )

        assert any(b.rule == "daily_loss_limit" for b in breaches)
        events = event_store.query(event_type="risk.portfolio_breach")
        assert len(events) >= 1

    def test_calc_position_writes_event(self, event_store):
        svc = RiskService(event_store)

        ps = svc.calc_and_record_position(
            code="002138",
            total_capital=450000,
            current_exposure_pct=0.2,
            price=15.0,
            market_multiplier=1.0,
            run_id="run_risk_003",
        )

        assert ps.shares > 0
        events = event_store.query(event_type="risk.position_sized")
        assert len(events) == 1
        assert events[0]["payload"]["code"] == "002138"
