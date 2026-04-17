"""
platform/runs.py — 运行生命周期管理

每次 pipeline 执行都有 run_id，记录开始/结束/状态/config_version。
支持幂等检查：同一 run_type + 同一天不重复执行。
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import timedelta
from typing import Optional

from hermes.platform.time import local_date_bounds_utc, local_now, utc_now
from hermes.platform.time import utc_now_iso


def _make_run_id(run_type: str) -> str:
    ts = local_now().strftime("%Y%m%d_%H%M%S")
    short = uuid.uuid4().hex[:6]
    return f"run_{run_type}_{ts}_{short}"


class RunJournal:
    """Track pipeline run lifecycle with idempotency checks."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def start_run(
        self,
        run_type: str,
        config_version: str,
        scope: str = "cn_a",
        data_cutoff: Optional[str] = None,
    ) -> str:
        """Create a new run record. Returns run_id."""
        run_id = _make_run_id(run_type)
        self._conn.execute(
            """INSERT INTO run_log
               (run_id, run_type, scope, config_version, data_cutoff,
                status, started_at)
               VALUES (?, ?, ?, ?, ?, 'running', ?)""",
            (run_id, run_type, scope, config_version, data_cutoff, utc_now_iso()),
        )

        # Mark config as activated if first use
        self._conn.execute(
            """UPDATE config_versions SET activated_at = ?
               WHERE config_version = ? AND activated_at IS NULL""",
            (utc_now_iso(), config_version),
        )
        return run_id

    def complete_run(
        self,
        run_id: str,
        artifacts: Optional[dict] = None,
    ) -> None:
        """Mark a run as completed."""
        self._conn.execute(
            """UPDATE run_log
               SET status = 'completed', finished_at = ?, artifacts_json = ?
               WHERE run_id = ?""",
            (
                utc_now_iso(),
                json.dumps(artifacts or {}, ensure_ascii=False, default=str),
                run_id,
            ),
        )

    def fail_run(self, run_id: str, error: str) -> None:
        """Mark a run as failed."""
        self._conn.execute(
            """UPDATE run_log
               SET status = 'failed', finished_at = ?, error_message = ?
               WHERE run_id = ?""",
            (utc_now_iso(), error[:4000], run_id),
        )

    def is_completed_today(self, run_type: str, scope: str = "cn_a") -> bool:
        """Idempotency check: has this run_type completed today?"""
        start_utc, end_utc = local_date_bounds_utc()
        row = self._conn.execute(
            """SELECT 1 FROM run_log
               WHERE run_type = ? AND scope = ? AND status = 'completed'
                 AND started_at >= ? AND started_at < ?
               LIMIT 1""",
            (run_type, scope, start_utc, end_utc),
        ).fetchone()
        return row is not None

    def get_last_run(
        self, run_type: str, date: Optional[str] = None
    ) -> Optional[dict]:
        """Get the most recent run of a given type, optionally filtered by date."""
        if date:
            start_utc, end_utc = local_date_bounds_utc(date)
            row = self._conn.execute(
                """SELECT * FROM run_log
                   WHERE run_type = ? AND started_at >= ? AND started_at < ?
                   ORDER BY started_at DESC LIMIT 1""",
                (run_type, start_utc, end_utc),
            ).fetchone()
        else:
            row = self._conn.execute(
                """SELECT * FROM run_log
                   WHERE run_type = ?
                   ORDER BY started_at DESC LIMIT 1""",
                (run_type,),
            ).fetchone()
        return dict(row) if row else None

    def get_failed_runs(self, days: int = 7) -> list[dict]:
        """Get recent failed runs for replay consideration."""
        cutoff = (utc_now() - timedelta(days=days)).isoformat()
        rows = self._conn.execute(
            """SELECT * FROM run_log
               WHERE status = 'failed' AND started_at >= ?
               ORDER BY started_at DESC""",
            (cutoff,),
        ).fetchall()
        return [dict(r) for r in rows]

    def list_runs(
        self,
        run_type: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 20,
    ) -> list[dict]:
        """List recent runs with optional filters."""
        clauses: list[str] = []
        params: list = []
        if run_type:
            clauses.append("run_type = ?")
            params.append(run_type)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = " AND ".join(clauses) if clauses else "1=1"
        params.append(limit)
        rows = self._conn.execute(
            f"SELECT * FROM run_log WHERE {where} ORDER BY started_at DESC LIMIT ?",
            params,
        ).fetchall()
        return [dict(r) for r in rows]
