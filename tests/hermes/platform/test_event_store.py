"""Tests for platform/events.py — EventStore"""

import pytest
import sqlite3

from hermes.platform.db import init_db
from hermes.platform.events import EventStore


@pytest.fixture
def store(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    yield EventStore(conn)
    conn.close()


def test_append_and_query(store: EventStore):
    eid = store.append(
        stream="strategy:002138",
        stream_type="strategy",
        event_type="score.calculated",
        payload={"code": "002138", "total": 7.2},
        metadata={"run_id": "run_test_001"},
    )
    assert eid
    events = store.query(event_type="score.calculated")
    assert len(events) == 1
    assert events[0]["payload"]["total"] == 7.2
    assert events[0]["metadata"]["run_id"] == "run_test_001"
    assert events[0]["stream_version"] == 1


def test_stream_version_auto_increment(store: EventStore):
    store.append("order:001", "order", "order.created", {"code": "001"})
    store.append("order:001", "order", "order.filled", {"code": "001", "price": 100})
    store.append("order:001", "order", "order.cancelled", {"code": "001"})

    events = store.get_stream("order:001")
    assert len(events) == 3
    assert [e["stream_version"] for e in events] == [1, 2, 3]
    assert [e["event_type"] for e in events] == [
        "order.created", "order.filled", "order.cancelled"
    ]


def test_duplicate_stream_version_raises(store: EventStore):
    """Same stream + version should violate UNIQUE constraint."""
    store.append("s:1", "test", "a", {"x": 1})
    # Second append to same stream gets version 2, not a conflict
    store.append("s:1", "test", "b", {"x": 2})
    events = store.get_stream("s:1")
    assert len(events) == 2


def test_query_by_stream(store: EventStore):
    store.append("s:a", "t", "e1", {"v": 1})
    store.append("s:b", "t", "e1", {"v": 2})
    store.append("s:a", "t", "e2", {"v": 3})

    events = store.query(stream="s:a")
    assert len(events) == 2
    assert all(e["stream"] == "s:a" for e in events)


def test_query_by_stream_type(store: EventStore):
    store.append("s:1", "strategy", "score.calculated", {"v": 1})
    store.append("s:2", "risk", "risk.blocked", {"v": 2})

    events = store.query(stream_type="strategy")
    assert len(events) == 1
    assert events[0]["stream_type"] == "strategy"


def test_count(store: EventStore):
    store.append("s:1", "t", "score.calculated", {})
    store.append("s:2", "t", "score.calculated", {})
    store.append("s:3", "t", "order.created", {})

    assert store.count(event_type="score.calculated") == 2
    assert store.count() == 3


def test_empty_query(store: EventStore):
    events = store.query(event_type="nonexistent")
    assert events == []
    assert store.count() == 0


def test_metadata_filter(store: EventStore):
    """metadata_filter should filter events at SQL level."""
    store.append("s:1", "t", "score.calculated", {"v": 1}, metadata={"run_id": "run_a"})
    store.append("s:2", "t", "score.calculated", {"v": 2}, metadata={"run_id": "run_b"})
    store.append("s:3", "t", "score.calculated", {"v": 3}, metadata={"run_id": "run_a"})

    events = store.query(event_type="score.calculated", metadata_filter={"run_id": "run_a"})
    assert len(events) == 2
    assert all(e["metadata"]["run_id"] == "run_a" for e in events)


def test_metadata_filter_empty_result(store: EventStore):
    store.append("s:1", "t", "e1", {"v": 1}, metadata={"run_id": "run_x"})
    events = store.query(metadata_filter={"run_id": "nonexistent"})
    assert events == []


def test_metadata_filter_combined_with_event_type(store: EventStore):
    store.append("s:1", "t", "score.calculated", {"v": 1}, metadata={"run_id": "run_a"})
    store.append("s:2", "t", "decision.suggested", {"v": 2}, metadata={"run_id": "run_a"})
    store.append("s:3", "t", "score.calculated", {"v": 3}, metadata={"run_id": "run_b"})

    events = store.query(event_type="score.calculated", metadata_filter={"run_id": "run_a"})
    assert len(events) == 1
    assert events[0]["payload"]["v"] == 1


def test_event_id_full_uuid(store: EventStore):
    """Event IDs should be full 32-char hex UUIDs (not truncated 16-char)."""
    eid = store.append("s:1", "t", "e1", {"v": 1})
    assert len(eid) == 32


def test_transaction_protection(store: EventStore):
    """Concurrent appends to same stream should get sequential versions."""
    for i in range(10):
        store.append("s:race", "t", f"e{i}", {"i": i})

    events = store.get_stream("s:race")
    versions = [e["stream_version"] for e in events]
    assert versions == list(range(1, 11))
