"""
risk/models.py — 风控领域模型
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Optional

from hermes.strategy.models import Style


@dataclass(frozen=True)
class RiskParams:
    style: Style
    stop_loss: float = 0.08
    trailing_stop: Optional[float] = None
    exit_ma: int = 20
    time_stop_days: int = 15
    absolute_stop_ma: Optional[int] = None
    stop_loss_anchor: Optional[str] = None


@dataclass(frozen=True)
class ExitSignal:
    code: str
    signal_type: str  # stop_loss / trailing_stop / time_stop / ma_exit / style_switch
    trigger_price: float
    current_price: float
    description: str
    urgency: str = "end_of_day"  # immediate / end_of_day / advisory


@dataclass(frozen=True)
class PositionSize:
    shares: int
    amount: float
    pct: float
    market_multiplier: float = 1.0


@dataclass(frozen=True)
class RiskBreach:
    rule: str
    current_value: float
    limit_value: float
    description: str
