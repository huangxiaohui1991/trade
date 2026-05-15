"""
market/mx/moni.py — 妙想模拟盘交易

提供 dispatch_trade_command() 用于 MXBroker 下单。
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
    _logger.warning("[moni] direct MX broker command is disabled; use bin/trade paper ...")
    return {
        "code": "-1",
        "message": "Direct MX broker command is disabled; use bin/trade paper ...",
        "command": command,
    }
