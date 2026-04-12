#!/usr/bin/env python3
"""
统一异常体系 — 交易系统

所有业务异常均继承自 TradeError，便于统一捕获和日志记录。
"""


class TradeError(Exception):
    """交易系统基异常"""
    pass


class ScoreError(TradeError):
    """评分引擎错误"""
    pass


class RiskModelError(TradeError):
    """风控模型错误"""
    pass


class BrokerError(TradeError):
    """券商/Broker 错误"""
    pass


class VaultError(TradeError):
    """Vault 读写错误"""
    pass


class PipelineError(TradeError):
    """Pipeline 执行错误"""
    pass


class DataSourceError(TradeError):
    """数据源错误（MX/akshare/新浪）"""
    pass


class ConfigError(TradeError):
    """配置错误"""
    pass
