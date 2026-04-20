from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ContinuationFilterConfig:
    amount_min: float = 2e8
    change_pct_min: float = 2.0
    close_near_high_min: float = 0.75
    max_intraday_retrace: float = 0.04
    volume_ratio_min: float = 1.2
    volume_ratio_max: float = 3.5
    require_above_ma5: bool = True
    exclude_long_upper_shadow: bool = True
    exclude_limit_up_locked: bool = True


@dataclass(frozen=True)
class ContinuationScoreConfig:
    strength_weight: float = 1.0
    continuity_weight: float = 1.0
    quality_weight: float = 1.0
    flow_weight: float = 0.5
    stability_weight: float = 0.7
    top_n: int = 3
    hold_days: tuple[int, int, int] = (1, 2, 3)
    overheat_change_pct: float = 8.0
    overheat_volume_ratio: float = 4.0
    overheat_deviation_rate: float = 8.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "hold_days", tuple(self.hold_days))


@dataclass(frozen=True)
class ContinuationFilterResult:
    qualified: bool
    reasons: list[str] = field(default_factory=list)
    close_near_high: float = 0.0
    intraday_retrace: float = 0.0


@dataclass(frozen=True)
class ContinuationScoreResult:
    code: str
    name: str
    qualified: bool
    strength_score: float = 0.0
    continuity_score: float = 0.0
    quality_score: float = 0.0
    flow_score: float = 0.0
    stability_score: float = 0.0
    overheat_penalty: float = 0.0
    notes: list[str] = field(default_factory=list)

    @property
    def total_score(self) -> float:
        raw = (
            self.strength_score
            + self.continuity_score
            + self.quality_score
            + self.flow_score
            + self.stability_score
        )
        return round(max(0.0, raw - self.overheat_penalty), 1)
