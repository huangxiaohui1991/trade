"""Tests for platform/runs.py — RunJournal"""

import pytest
import sqlite3

from hermes.platform.db import init_db
from hermes.platform.config import ConfigRegistry
from hermes.platform.runs import RunJournal


@pytest.fixture
def journal(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")

    # Need a config version for foreign-key-like references
    registry = ConfigRegistry()
    snapshot = registry.freeze(conn)

    j = RunJournal(conn)
    yield j, conn, snapshot.version
    conn.close()


def test_start_and_complete(journal):
    j, conn, cv = journal
    run_id = j.start_run("morning", cv)
    assert run_id.startswith("run_morning_")

    last = j.get_last_run("morning")
    assert last is not None
    assert last["status"] == "running"

    j.complete_run(run_id, artifacts={"scores": 10})

    last = j.get_last_run("morning")
    assert last["status"] == "completed"
    assert last["finished_at"] is not None


def test_fail_run(journal):
    j, conn, cv = journal
    run_id = j.start_run("evening", cv)
    j.fail_run(run_id, "network timeout")

    last = j.get_last_run("evening")
    assert last["status"] == "failed"
    assert "network timeout" in last["error_message"]

    failed = j.get_failed_runs(days=1)
    assert len(failed) == 1
    assert failed[0]["run_id"] == run_id


def test_is_completed_today(journal):
    j, conn, cv = journal

    assert j.is_completed_today("scoring") is False

    run_id = j.start_run("scoring", cv)
    assert j.is_completed_today("scoring") is False  # still running

    j.complete_run(run_id)
    assert j.is_completed_today("scoring") is True


def test_idempotency_check(journal):
    """A completed run should prevent re-execution on the same day."""
    j, conn, cv = journal

    run_id = j.start_run("morning", cv)
    j.complete_run(run_id)

    # Simulating idempotency check before starting a new run
    assert j.is_completed_today("morning") is True
    # Different run_type should not be affected
    assert j.is_completed_today("evening") is False


def test_list_runs(journal):
    j, conn, cv = journal
    j.start_run("morning", cv)
    j.start_run("evening", cv)

    runs = j.list_runs()
    assert len(runs) == 2

    runs = j.list_runs(run_type="morning")
    assert len(runs) == 1
    assert runs[0]["run_type"] == "morning"


def test_multiple_runs_same_type(journal):
    j, conn, cv = journal
    r1 = j.start_run("scoring", cv)
    j.fail_run(r1, "error 1")

    r2 = j.start_run("scoring", cv)
    j.complete_run(r2)

    # is_completed_today should be True because r2 succeeded
    assert j.is_completed_today("scoring") is True

    # get_last_run should return the most recent
    last = j.get_last_run("scoring")
    assert last["run_id"] == r2
    assert last["status"] == "completed"
