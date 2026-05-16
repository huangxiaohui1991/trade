"""Minimal MX paper-broker compatibility client for legacy state scripts."""

from __future__ import annotations


class MXMoni:
    """No-network fallback used by legacy state snapshot helpers in tests."""

    def positions(self) -> dict:
        return {"posList": []}

    def balance(self) -> dict:
        return {"totalAssets": 0, "availBalance": 0, "totalPosValue": 0}
