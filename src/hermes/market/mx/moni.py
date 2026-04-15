"""
market/mx/moni.py — 妙想模拟盘交易

提供 dispatch_trade_command() 用于 MXBroker 下单。
从 V1 scripts/mx/cli_tools.py 的 dispatch_mx_command 简化迁移。
"""

from __future__ import annotations

import logging
from typing import Any, Dict

_logger = logging.getLogger(__name__)


def dispatch_trade_command(command: str, **kwargs: Any) -> Dict[str, Any]:
    """
    调用妙想模拟盘交易命令。

    command: "mx.moni.buy" | "mx.moni.sell"
    kwargs: stock_code, quantity, use_market_price, price
    """
    try:
        # 尝试用 V1 的 dispatch（如果 scripts/ 还在 PYTHONPATH 中）
        from scripts.mx.cli_tools import dispatch_mx_command
        return dispatch_mx_command(command, **kwargs)
    except ImportError:
        _logger.warning("[moni] V1 scripts.mx.cli_tools 已废弃且不可用，跳过")

    # Fallback: 直接调 MXMoni API
    try:
        from hermes.market.mx.client import MXBaseClient
        # MXMoni 继承 MXBaseClient，endpoint 不同
        _logger.warning(f"[moni] V1 cli_tools not available, command={command} skipped")
        return {"code": "-1", "message": "MXMoni not available (V1 scripts removed)"}
    except Exception as e:
        return {"code": "-1", "message": str(e)}
