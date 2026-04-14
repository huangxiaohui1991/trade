"""Test all four pipelines with mock data (no network)."""

import pytest
from pathlib import Path

from hermes.platform.db import init_db, connect
from hermes.platform.events import EventStore
from hermes.platform.runs import RunJournal
from hermes.market.service import MarketService
from hermes.market.store import MarketStore
from hermes.strategy.models import ScoringWeights
from hermes.strategy.scorer import Scorer
from hermes.strategy.decider import Decider
from hermes.strategy.service import StrategyService
from hermes.risk.service import RiskService
from hermes.execution.service import ExecutionService, SimulatedBroker
from hermes.reporting.projectors import ProjectionUpdater
from hermes.reporting.reports import ReportGenerator
from hermes.reporting.obsidian import ObsidianProjector
from hermes.pipeline.context import PipelineContext


@pytest.fixture
def ctx(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)
    es = EventStore(conn)
    vault = tmp_path / "vault"
    vault.mkdir()

    scorer = Scorer(weights=ScoringWeights(), veto_rules=["below_ma20"])
    decider = Decider(buy_threshold=6.5, watch_threshold=5.0)

    c = PipelineContext(
        conn=conn, event_store=es, run_journal=RunJournal(conn),
        config_snapshot=None,
        market_svc=MarketService(store=MarketStore(conn)),
        strategy_svc=StrategyService(scorer, decider, es),
        risk_svc=RiskService(es),
        exec_svc=ExecutionService(es, conn, broker=SimulatedBroker()),
        projector=ProjectionUpdater(es, conn),
        reporter=ReportGenerator(es, conn),
        obsidian=ObsidianProjector(es, conn, vault_path=str(vault)),
        vault_path=str(vault),
    )

    # Seed some pool data
    conn.execute(
        "INSERT INTO projection_candidate_pool (code, pool_tier, name, score, added_at) VALUES (?, ?, ?, ?, ?)",
        ("002138", "core", "双环传动", 7.5, "2026-04-10"),
    )

    yield c
    conn.close()


class TestMorningPipeline:
    def test_runs_without_error(self, ctx):
        from hermes.pipeline.morning import run
        result = run(ctx, "run_morning_test")
        assert "signal" in result
        assert isinstance(result["risk_alerts"], list)

    def test_writes_obsidian(self, ctx):
        from hermes.pipeline.morning import run
        run(ctx, "run_morning_test")
        assert (Path(ctx.vault_path) / "04-决策" / "今日决策.md").exists()
        assert (Path(ctx.vault_path) / "01-状态" / "持仓" / "持仓概览.md").exists()


class TestScoringPipeline:
    def test_runs_without_error(self, ctx):
        from hermes.pipeline.scoring import run
        result = run(ctx, "run_scoring_test")
        # No network, so scored=0 (MarketService returns empty snapshots)
        # But pipeline should not crash
        assert "scored" in result

    def test_writes_obsidian(self, ctx):
        from hermes.pipeline.scoring import run
        run(ctx, "run_scoring_test")
        vault = Path(ctx.vault_path)
        assert (vault / "01-状态" / "池子" / "核心池.md").exists()
        assert (vault / "01-状态" / "池子" / "观察池.md").exists()


class TestEveningPipeline:
    def test_runs_without_error(self, ctx):
        from hermes.pipeline.evening import run
        result = run(ctx, "run_evening_test")
        assert "signal" in result

    def test_writes_obsidian(self, ctx):
        from hermes.pipeline.evening import run
        run(ctx, "run_evening_test")
        assert (Path(ctx.vault_path) / "01-状态" / "持仓" / "持仓概览.md").exists()


class TestWeeklyPipeline:
    def test_runs_without_error(self, ctx):
        from hermes.pipeline.weekly import run
        result = run(ctx, "run_weekly_test")
        assert "week" in result
        assert result["buy_count"] == 0

    def test_writes_obsidian(self, ctx):
        from hermes.pipeline.weekly import run
        result = run(ctx, "run_weekly_test")
        vault = Path(ctx.vault_path)
        week_files = list((vault / "03-分析" / "周复盘").glob("*.md"))
        assert len(week_files) >= 1
