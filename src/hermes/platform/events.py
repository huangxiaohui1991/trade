"""
platform/events.py — append-only EventStore

业务事实只 INSERT 不 UPDATE/DELETE。
每个事件属于一个 stream，stream 内 version 自动递增。
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Optional


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id() -> str:
    return uuid.uuid4().hex


class EventStore:
    """Append-only event log backed by SQLite."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def append(
        self,
        stream: str,
        stream_type: str,
        event_type: str,
        payload: dict,
        metadata: Optional[dict] = None,
    ) -> str:
        """
        追加一条事件。自动递增 stream_version。
        使用 BEGIN IMMEDIATE 保证 version 查询和 INSERT 的原子性。

        Returns:
            event_id
        """
        metadata = metadata or {}
        event_id = _new_id()

        # BEGIN IMMEDIATE 获取写锁，防止并发 version 冲突
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            row = self._conn.execute(
                "SELECT MAX(stream_version) FROM event_log WHERE stream = ?",
                (stream,),
            ).fetchone()
            next_version = (row[0] or 0) + 1 if row else 1

            self._conn.execute(
                """INSERT INTO event_log
                   (event_id, stream, stream_type, stream_version,
                    event_type, payload_json, metadata_json, occurred_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    event_id,
                    stream,
                    stream_type,
                    next_version,
                    event_type,
                    json.dumps(payload, ensure_ascii=False, default=str),
                    json.dumps(metadata, ensure_ascii=False, default=str),
                    _now_iso(),
                ),
            )
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise
        return event_id

    def query(
        self,
        stream: Optional[str] = None,
        stream_type: Optional[str] = None,
        event_type: Optional[str] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
        limit: int = 1000,
        metadata_filter: Optional[dict] = None,
    ) -> list[dict]:
        """Query events with optional filters.

        Args:
            metadata_filter: 可选的 metadata 字段过滤，如 {"run_id": "xxx"}。
                使用 json_extract 在 SQL 层过滤，避免全量拉取后内存过滤。
        """
        clauses: list[str] = []
        params: list = []

        if stream:
            clauses.append("stream = ?")
            params.append(stream)
        if stream_type:
            clauses.append("stream_type = ?")
            params.append(stream_type)
        if event_type:
            clauses.append("event_type = ?")
            params.append(event_type)
        if since:
            clauses.append("occurred_at >= ?")
            params.append(since)
        if until:
            clauses.append("occurred_at <= ?")
            params.append(until)
        if metadata_filter:
            for key, value in metadata_filter.items():
                clauses.append(f"json_extract(metadata_json, '$.{key}') = ?")
                params.append(value)

        where = " AND ".join(clauses) if clauses else "1=1"
        sql = f"""SELECT * FROM event_log
                  WHERE {where}
                  ORDER BY occurred_at, stream_version
                  LIMIT ?"""
        params.append(limit)

        rows = self._conn.execute(sql, params).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def get_stream(self, stream: str) -> list[dict]:
        """Get all events for a stream, ordered by version."""
        rows = self._conn.execute(
            "SELECT * FROM event_log WHERE stream = ? ORDER BY stream_version",
            (stream,),
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def count(
        self,
        event_type: Optional[str] = None,
        since: Optional[str] = None,
    ) -> int:
        """Count events matching filters."""
        clauses: list[str] = []
        params: list = []
        if event_type:
            clauses.append("event_type = ?")
            params.append(event_type)
        if since:
            clauses.append("occurred_at >= ?")
            params.append(since)
        where = " AND ".join(clauses) if clauses else "1=1"
        row = self._conn.execute(
            f"SELECT COUNT(*) FROM event_log WHERE {where}", params
        ).fetchone()
        return row[0]

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> dict:
        d = dict(row)
        if "payload_json" in d:
            d["payload"] = json.loads(d.pop("payload_json"))
        if "metadata_json" in d:
            d["metadata"] = json.loads(d.pop("metadata_json"))
        return d
