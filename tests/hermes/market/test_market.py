"""Tests for market/store.py and market/service.py"""

import asyncio
import pytest

from hermes.market.models import (
    FinancialReport,
    FundFlow,
    IndexQuote,
    SentimentData,
    StockQuote,
    StockSnapshot,
)
from hermes.market.store import MarketStore
from hermes.market.service import MarketService
from hermes.platform.db import init_db, connect


@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)
    yield conn
    conn.close()


@pytest.fixture
def store(db):
    return MarketStore(db)


# ---------------------------------------------------------------------------
# MarketStore tests
# ---------------------------------------------------------------------------

class TestMarketStore:
    def test_save_and_get_observation(self, store):
        store.save_observation("test", "quote", "002138", {"price": 15.0})
        result = store.get_latest_observation("002138", "quote")
        assert result is not None
        assert result["price"] == 15.0

    def test_ttl_expired(self, store):
        store.save_observation("test", "quote", "002138", {"price": 15.0})
        # TTL=0 means always expired
        result = store.get_latest_observation("002138", "quote", max_age_seconds=0)
        assert result is None

    def test_ttl_valid(self, store):
        store.save_observation("test", "quote", "002138", {"price": 15.0})
        result = store.get_latest_observation("002138", "quote", max_age_seconds=3600)
        assert result is not None

    def test_get_cached(self, store):
        store.save_observation("test", "financial", "002138", {"roe": 12.0})
        result = store.get_cached("002138", "financial")
        assert result is not None
        assert result["roe"] == 12.0

    def test_no_observation(self, store):
        result = store.get_latest_observation("999999", "quote")
        assert result is None


# ---------------------------------------------------------------------------
# Mock providers for MarketService tests
# ---------------------------------------------------------------------------

class MockMarketProvider:
    def __init__(self, quotes=None):
        self._quotes = quotes or {}

    async def get_realtime(self, codes):
        return {c: self._quotes[c] for c in codes if c in self._quotes}

    async def get_kline(self, code, period="daily", count=120):
        return None

    async def get_index(self, symbols):
        return {}


class MockFinancialProvider:
    def __init__(self, data=None):
        self._data = data or {}

    async def get_financial(self, code):
        return self._data.get(code)


class MockFlowProvider:
    def __init__(self, data=None):
        self._data = data or {}

    async def get_fund_flow(self, code, days=5):
        return self._data.get(code)


class MockSentimentProvider:
    async def search_news(self, query):
        return SentimentData(score=2.0, detail="mock")


class FailingProvider:
    """Always raises."""
    async def get_realtime(self, codes):
        raise ConnectionError("mock fail")

    async def get_kline(self, code, period="daily", count=120):
        raise ConnectionError("mock fail")

    async def get_index(self, symbols):
        raise ConnectionError("mock fail")

    async def get_financial(self, code):
        raise ConnectionError("mock fail")

    async def get_fund_flow(self, code, days=5):
        raise ConnectionError("mock fail")

    async def search_news(self, query):
        raise ConnectionError("mock fail")


# ---------------------------------------------------------------------------
# MarketService tests
# ---------------------------------------------------------------------------

class TestMarketService:
    def test_collect_snapshot(self, store):
        quote = StockQuote(
            code="002138", name="双环传动", price=15.0,
            open=14.8, high=15.2, low=14.7, close=15.0,
            volume=5000000, amount=7.5e8, change_pct=1.5,
        )
        svc = MarketService(
            market_providers=[MockMarketProvider({"002138": quote})],
            financial_providers=[MockFinancialProvider({"002138": FinancialReport(roe=12.0)})],
            flow_providers=[MockFlowProvider({"002138": FundFlow(net_inflow_1d=6e8)})],
            sentiment_providers=[MockSentimentProvider()],
            store=store,
        )

        snap = asyncio.get_event_loop().run_until_complete(
            svc.collect_snapshot("002138", "双环传动", run_id="run_test")
        )

        assert snap.code == "002138"
        assert snap.quote is not None
        assert snap.quote.price == 15.0
        assert snap.financial is not None
        assert snap.financial.roe == 12.0
        assert snap.flow is not None
        assert snap.sentiment is not None

    def test_collect_batch(self, store):
        q1 = StockQuote(code="001", name="A", price=10.0, open=10, high=10, low=10, close=10, volume=1000, amount=1e7, change_pct=0)
        q2 = StockQuote(code="002", name="B", price=20.0, open=20, high=20, low=20, close=20, volume=2000, amount=2e7, change_pct=0)

        svc = MarketService(
            market_providers=[MockMarketProvider({"001": q1, "002": q2})],
            store=store,
        )

        snaps = asyncio.get_event_loop().run_until_complete(
            svc.collect_batch([{"code": "001", "name": "A"}, {"code": "002", "name": "B"}])
        )

        assert len(snaps) == 2
        assert snaps[0].code == "001"
        assert snaps[1].code == "002"

    def test_fallback_on_failure(self, store):
        """First provider fails, second succeeds."""
        quote = StockQuote(
            code="002138", name="双环传动", price=15.0,
            open=14.8, high=15.2, low=14.7, close=15.0,
            volume=5000000, amount=7.5e8, change_pct=1.5,
        )
        svc = MarketService(
            market_providers=[FailingProvider(), MockMarketProvider({"002138": quote})],
            financial_providers=[FailingProvider(), MockFinancialProvider({"002138": FinancialReport(roe=10.0)})],
            store=store,
        )

        snap = asyncio.get_event_loop().run_until_complete(
            svc.collect_snapshot("002138", "双环传动")
        )

        assert snap.quote is not None
        assert snap.quote.price == 15.0
        assert snap.financial is not None

    def test_all_providers_fail(self, store):
        """All providers fail → snapshot with None fields."""
        svc = MarketService(
            market_providers=[FailingProvider()],
            financial_providers=[FailingProvider()],
            flow_providers=[FailingProvider()],
            sentiment_providers=[FailingProvider()],
            store=store,
        )

        snap = asyncio.get_event_loop().run_until_complete(
            svc.collect_snapshot("002138", "双环传动")
        )

        assert snap.code == "002138"
        assert snap.quote is None
        assert snap.financial is None

    def test_observation_saved(self, store):
        svc = MarketService(
            market_providers=[MockMarketProvider({})],
            store=store,
        )

        asyncio.get_event_loop().run_until_complete(
            svc.collect_snapshot("002138", "双环传动", run_id="run_obs")
        )

        obs = store.get_latest_observation("002138", "snapshot")
        assert obs is not None
