"""Service composition root contracts."""

from __future__ import annotations

from astock_trading.platform.db import connect, init_db


def test_build_market_service_uses_common_provider_order(tmp_path):
    from astock_trading.market.hk_adapters import AkShareHKFinancialAdapter, AkShareHKMarketAdapter
    from astock_trading.market.service import MarketService
    from astock_trading.platform.service_factory import build_market_service

    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        service = build_market_service(conn)
    finally:
        conn.close()

    assert isinstance(service, MarketService)
    assert any(isinstance(provider, AkShareHKMarketAdapter) for provider in service._market)
    assert any(isinstance(provider, AkShareHKFinancialAdapter) for provider in service._financial)


def test_pipeline_context_uses_shared_market_service_builder(tmp_path, monkeypatch):
    from astock_trading.market.service import MarketService
    from astock_trading.platform import service_factory
    from astock_trading.pipeline.context import build_context

    calls = []

    def fake_build_market_service(conn):
        calls.append(conn)
        return MarketService()

    monkeypatch.setattr(service_factory, "build_market_service", fake_build_market_service)

    ctx = build_context(tmp_path / "test.db")
    try:
        assert calls == [ctx.conn]
        assert isinstance(ctx.market_svc, MarketService)
    finally:
        ctx.conn.close()


def test_mcp_init_uses_shared_runtime_factory(monkeypatch):
    from types import SimpleNamespace

    import astock_trading.platform.mcp_server as srv
    from astock_trading.platform import service_factory

    fake = SimpleNamespace(
        conn=object(),
        event_store=object(),
        run_journal=object(),
        exec_svc=object(),
        reporter=object(),
        market_svc=object(),
        strategy_svc=object(),
        config_snapshot=object(),
    )

    monkeypatch.setattr(service_factory, "build_runtime_services", lambda: fake)
    srv._conn = None

    srv._init()

    try:
        assert srv._conn is fake.conn
        assert srv._event_store is fake.event_store
        assert srv._run_journal is fake.run_journal
        assert srv._exec_svc is fake.exec_svc
        assert srv._report_gen is fake.reporter
        assert srv._market_svc is fake.market_svc
        assert srv._strategy_svc is fake.strategy_svc
        assert srv._config_snapshot is fake.config_snapshot
    finally:
        srv._conn = None
        srv._event_store = None
        srv._run_journal = None
        srv._exec_svc = None
        srv._report_gen = None
        srv._market_svc = None
        srv._strategy_svc = None
        srv._config_snapshot = None


def test_paper_mcp_tools_have_focused_registrar():
    from astock_trading.platform.mcp_tools.paper import register_paper_tools

    assert callable(register_paper_tools)
