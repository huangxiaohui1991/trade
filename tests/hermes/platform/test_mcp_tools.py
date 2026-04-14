"""Tests for MCP Server tools — unit tests calling tool logic directly."""

import json
import pytest

from hermes.platform.db import init_db, connect
from hermes.platform.events import EventStore
from hermes.platform.runs import RunJournal
from hermes.execution.service import ExecutionService
from hermes.reporting.reports import ReportGenerator


@pytest.fixture
def setup_mcp(tmp_path):
    """Set up the MCP server globals for testing."""
    import hermes.platform.mcp_server as srv
    from hermes.market.service import MarketService
    from hermes.market.store import MarketStore
    from hermes.strategy.models import ScoringWeights
    from hermes.strategy.scorer import Scorer
    from hermes.strategy.decider import Decider
    from hermes.strategy.service import StrategyService

    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)

    srv._conn = conn
    srv._event_store = EventStore(conn)
    srv._run_journal = RunJournal(conn)
    srv._exec_svc = ExecutionService(srv._event_store, conn)
    srv._report_gen = ReportGenerator(srv._event_store, conn)
    srv._market_svc = MarketService(store=MarketStore(conn))
    srv._config_snapshot = None

    scorer = Scorer(weights=ScoringWeights(), veto_rules=[])
    decider = Decider()
    srv._strategy_svc = StrategyService(scorer, decider, srv._event_store)

    yield srv

    conn.close()
    srv._conn = None
    srv._event_store = None
    srv._market_svc = None
    srv._strategy_svc = None


class TestMCPTools:
    def test_trade_portfolio_empty(self, setup_mcp):
        srv = setup_mcp
        result = json.loads(srv.trade_portfolio())
        assert result["holding_count"] == 0

    def test_trade_portfolio_with_position(self, setup_mcp):
        srv = setup_mcp
        srv._exec_svc.execute_buy("002138", "双环传动", 100, 1500, "momentum", "run_1")

        result = json.loads(srv.trade_portfolio())
        assert result["holding_count"] == 1
        assert result["positions"][0]["code"] == "002138"

    def test_trade_score_history_empty(self, setup_mcp):
        srv = setup_mcp
        result = json.loads(srv.trade_score_history("002138"))
        assert result["code"] == "002138"
        assert result["history"] == []

    def test_trade_score_history_with_data(self, setup_mcp):
        srv = setup_mcp
        srv._event_store.append(
            stream="strategy:002138", stream_type="strategy",
            event_type="score.calculated",
            payload={"code": "002138", "total_score": 7.5, "style": "momentum", "veto_triggered": False},
            metadata={"run_id": "run_1"},
        )

        result = json.loads(srv.trade_score_history("002138"))
        assert len(result["history"]) == 1
        assert result["history"][0]["total_score"] == 7.5

    def test_trade_trade_events_empty(self, setup_mcp):
        result = json.loads(setup_mcp.trade_trade_events())
        assert result["count"] == 0
        assert result["trades"] == []

    def test_trade_calc_position(self, setup_mcp):
        result = json.loads(setup_mcp.trade_calc_position("002138", 7.5, 15.0))
        assert result["code"] == "002138"
        assert result["shares"] > 0
        assert result["shares"] % 100 == 0

    def test_trade_market_signal_fallback(self, setup_mcp):
        """When V1 market_timer is unavailable, should return fallback."""
        result = json.loads(setup_mcp.trade_market_signal())
        assert "signal" in result
