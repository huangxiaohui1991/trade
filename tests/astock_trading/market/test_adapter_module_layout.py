"""Adapter module layout contracts."""

from __future__ import annotations


def test_adapter_compatibility_facade_reexports_split_modules():
    from astock_trading.market import (
        a_stock_adapters,
        adapters,
        akshare_adapters,
        baostock_adapters,
        hk_adapters,
        opencli_adapters,
    )

    assert adapters.OpenCliFinanceAdapter is opencli_adapters.OpenCliFinanceAdapter
    assert adapters.OpenCliXueqiuAdapter is opencli_adapters.OpenCliXueqiuAdapter
    assert adapters.TencentFinancialAdapter is a_stock_adapters.TencentFinancialAdapter
    assert adapters.BaiduFundFlowAdapter is a_stock_adapters.BaiduFundFlowAdapter
    assert adapters.AStockSignalAdapter is a_stock_adapters.AStockSignalAdapter
    assert adapters.MootdxMarketAdapter is a_stock_adapters.MootdxMarketAdapter
    assert adapters.AkShareMarketAdapter is akshare_adapters.AkShareMarketAdapter
    assert adapters.AkShareFinancialAdapter is akshare_adapters.AkShareFinancialAdapter
    assert adapters.AkShareFlowAdapter is akshare_adapters.AkShareFlowAdapter
    assert adapters.MXSentimentAdapter is akshare_adapters.MXSentimentAdapter
    assert adapters.MXScreenerAdapter is akshare_adapters.MXScreenerAdapter
    assert adapters.MXMarketAdapter is akshare_adapters.MXMarketAdapter
    assert adapters.BaoStockMarketAdapter is baostock_adapters.BaoStockMarketAdapter
    assert adapters.AkShareHKMarketAdapter is hk_adapters.AkShareHKMarketAdapter
    assert adapters.AkShareHKFinancialAdapter is hk_adapters.AkShareHKFinancialAdapter
    assert adapters.is_hk_code is hk_adapters.is_hk_code
    assert adapters.normalize_hk_code is hk_adapters.normalize_hk_code
