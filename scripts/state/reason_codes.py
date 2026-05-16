"""Reason-code normalization compatibility helpers."""

from __future__ import annotations


def normalize_reason_code(code: str, *, category: str = "") -> str:
    """Return a stable normalized reason-code string."""
    return str(code or "").strip()
