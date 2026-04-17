"""Tests for platform/db.py migration flow."""

from __future__ import annotations

import sqlite3

from hermes.platform.db import connect, get_schema_version, init_db


def test_init_db_sets_latest_schema_version(tmp_path):
    db_path = tmp_path / "test.db"

    init_db(db_path)

    conn = connect(db_path)
    try:
        version = get_schema_version(conn)
        assert version == 2

        columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(projection_positions)")
        }
        assert "currency" in columns
    finally:
        conn.close()


def test_init_db_migrates_v1_projection_positions(tmp_path):
    db_path = tmp_path / "legacy.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """CREATE TABLE projection_positions (
                   code TEXT PRIMARY KEY,
                   name TEXT NOT NULL,
                   style TEXT NOT NULL,
                   shares INTEGER NOT NULL,
                   avg_cost_cents INTEGER NOT NULL,
                   entry_date TEXT NOT NULL,
                   entry_day_low_cents INTEGER,
                   stop_loss_cents INTEGER,
                   take_profit_cents INTEGER,
                   highest_since_entry_cents INTEGER,
                   current_price_cents INTEGER,
                   unrealized_pnl_cents INTEGER,
                   updated_at TEXT NOT NULL
               )"""
        )
        conn.execute(
            "CREATE TABLE _schema_version (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)"
        )
        conn.execute(
            "INSERT INTO _schema_version (version, applied_at) VALUES (1, '2026-01-01T00:00:00+00:00')"
        )
        conn.commit()
    finally:
        conn.close()

    init_db(db_path)

    conn = connect(db_path)
    try:
        version = get_schema_version(conn)
        assert version == 2

        columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(projection_positions)")
        }
        assert "currency" in columns
    finally:
        conn.close()
