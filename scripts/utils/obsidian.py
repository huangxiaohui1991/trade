"""Minimal Obsidian vault adapter for legacy script compatibility."""

from __future__ import annotations

from pathlib import Path


class ObsidianVault:
    def __init__(self, vault_path: str | None = None):
        self.vault_path = vault_path or str(Path(__file__).resolve().parents[2] / "trade-vault")

    def read_portfolio(self) -> dict:
        return {
            "meta": {},
            "account_overview": [],
            "cn_a_holdings": [],
            "legacy_cn_holdings": [],
            "hk_legacy_holdings": [],
        }

    def sync_pool_projection(self, entries: list[dict], metadata: dict | None = None) -> dict:
        return {}
