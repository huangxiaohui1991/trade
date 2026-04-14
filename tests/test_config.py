"""Tests for platform/config.py — ConfigRegistry"""

import pytest
import sqlite3

from hermes.platform.db import init_db
from hermes.platform.config import ConfigRegistry


@pytest.fixture
def conn(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    c = sqlite3.connect(str(db_path))
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    yield c
    c.close()


def test_freeze_creates_version(conn):
    """freeze() should create a config_versions row and return a snapshot."""
    registry = ConfigRegistry()  # uses real config/ dir
    snapshot = registry.freeze(conn)

    assert snapshot.version.startswith("v")
    assert len(snapshot.hash) == 16
    assert isinstance(snapshot.data, dict)
    assert "strategy" in snapshot.data  # strategy.yaml should be loaded


def test_freeze_idempotent(conn):
    """Freezing the same config twice should return the same version."""
    registry = ConfigRegistry()
    s1 = registry.freeze(conn)
    s2 = registry.freeze(conn)

    assert s1.version == s2.version
    assert s1.hash == s2.hash


def test_get_version_roundtrip(conn):
    """freeze → get_version should return the same data."""
    registry = ConfigRegistry()
    snapshot = registry.freeze(conn)

    loaded = registry.get_version(conn, snapshot.version)
    assert loaded is not None
    assert loaded == snapshot.data


def test_get_nonexistent_version(conn):
    registry = ConfigRegistry()
    assert registry.get_version(conn, "v_nonexistent") is None


def test_snapshot_get_nested(conn):
    registry = ConfigRegistry()
    snapshot = registry.freeze(conn)

    # strategy.yaml has scoring.weights
    weights = snapshot.get("strategy", "scoring", "weights")
    assert weights is not None or snapshot.get("strategy", "scoring") is not None

    # nonexistent key returns default
    assert snapshot.get("nonexistent", "key", default=42) == 42


def test_list_versions(conn):
    registry = ConfigRegistry()
    registry.freeze(conn)

    versions = registry.list_versions(conn)
    assert len(versions) >= 1
    assert "config_version" in versions[0]
    assert "config_hash" in versions[0]
