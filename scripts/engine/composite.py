#!/usr/bin/env python3
"""
engine/composite.py — 综合决策引擎

职责：
  将四维评分 × 大盘信号 → 最终决策

  决策矩阵：
    评分 ≥7 + GREEN → BUY（正常仓位）
    评分 ≥7 + YELLOW → BUY（半仓）
    评分 ≥5 + GREEN → HOLD / 观察
    评分 <5 + GREEN → SELL / 不买
    评分 + RED → SELL / 不买
    评分 + CLEAR → 清仓观望

  同时输出：
    - 仓位建议（占总资金百分比）
    - 止损价
    - 止盈目标
    - 风控备注

用法：
  from scripts.engine.composite import CompositeDecider
  decider = CompositeDecider()
  decision = decider.decide(stock_code, four_dimensional_score)
  print(decision.action, decision.position_pct, decision.stop_loss)
"""

import os
import sys
import warnings
from dataclasses import dataclass, field
from typing import Optional, Literal

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

os.environ["TQDM_DISABLE"] = "1"
warnings.filterwarnings("ignore")

from scripts.engine.market_timer import MarketTimer, get_signal as get_market_signal
from scripts.utils.config_loader import get_strategy
from scripts.utils.logger import get_logger

_logger = get_logger("composite")


# ---------------------------------------------------------------------------
# 决策结果
# ---------------------------------------------------------------------------

@dataclass
class Decision:
    """决策结果"""
    action: Literal["BUY", "HOLD", "SELL", "WATCH", "CLEAR"]
    confidence: float          # 0-10 置信度
    position_pct: float       # 建议仓位占总资金百分比（0-1）
    position_amount: float     # 建议买入金额（绝对值）
    stop_loss_pct: float      # 止损百分比
    stop_loss_price: Optional[float] = None
    take_profit_pct: float = 0.15   # 止盈百分比（默认15%）
    take_profit_price: Optional[float] = field(default=None)
    market_multiplier: float = 1.0  # 大盘仓位系数
    veto_reasons: list = field(default_factory=list)
    notes: list = field(default_factory=list)


# ---------------------------------------------------------------------------
# CompositeDecider
# ---------------------------------------------------------------------------

class CompositeDecider:
    """
    综合决策引擎

  决策流程：
    1. 获取大盘信号 → 仓位系数
    2. 读取 strategy.yaml 评分阈值
    3. 组合评分 + 大盘信号 → 最终决策
    4. 结合风控参数 → 仓位、止损、止盈
    """

    def __init__(self, capital: Optional[float] = None):
        self.strategy = get_strategy()
        self.scoring_cfg = self.strategy.get("scoring", {})
        self.risk_cfg = self.strategy.get("risk", {})
        self.position_cfg = self.risk_cfg.get("position", {})

        # 总资金（从 strategy.yaml 或传入）
        self.total_capital = capital or self.strategy.get("capital", 450286)

        self.market_timer = MarketTimer()

        # 评分阈值
        self.thresholds = self.scoring_cfg.get("thresholds", {})
        self.buy_threshold = self.thresholds.get("buy", 7)
        self.watch_threshold = self.thresholds.get("watch", 5)
        self.reject_threshold = self.thresholds.get("reject", 4)

        # 风控参数
        self.stop_loss_pct = self.risk_cfg.get("stop_loss", 0.04)
        self.absolute_stop_pct = self.risk_cfg.get("absolute_stop", 0.07)
        self.t1_pct = self.risk_cfg.get("take_profit", {}).get("t1_pct", 0.15)
        self.single_max_pct = self.position_cfg.get("single_max", 0.20)
        self.total_max_pct = self.position_cfg.get("total_max", 0.60)
        self.weekly_max = self.position_cfg.get("weekly_max", 2)

        # veto 规则
        self.veto_rules = set(self.scoring_cfg.get("veto", []))

    # ---------------------------------------------------------------------------
    # 公开接口
    # ---------------------------------------------------------------------------

    def decide(self, stock_code: str, score: float,
               current_price: float,
               market_signal: Optional[str] = None,
               veto_signals: Optional[list] = None) -> Decision:
        """
        综合决策

        Args:
            stock_code: 股票代码
            score: 四维评分（0-10）
            current_price: 当前价格
            market_signal: 大盘信号（默认自动获取）
            veto_signals: 一票否决信号列表（如 below_ma20, limit_up_today 等）

        Returns:
            Decision 对象
        """
        if market_signal is None:
            market_signal = self.market_timer.get_signal()

        market_multiplier = self.market_timer.get_position_multiplier()
        veto_signals = veto_signals or []
        notes = []
        veto_reasons = []

        # 1. veto 检查
        for signal in veto_signals:
            if signal in self.veto_rules:
                veto_reasons.append(f"veto:{signal}")

        if veto_reasons:
            return Decision(
                action="CLEAR",
                confidence=0,
                position_pct=0,
                position_amount=0,
                stop_loss_pct=self.stop_loss_pct,
                market_multiplier=market_multiplier,
                veto_reasons=veto_reasons,
                notes=["一票否决，不操作"],
            )

        # 2. 大盘信号判断
        if market_signal == "CLEAR":
            action = "CLEAR"
            market_notes = ["MA60下方15日+，清仓观望"]
            pos_pct = 0.0
        elif market_signal == "RED":
            action = "SELL"
            market_notes = ["RED信号，清仓"]
            pos_pct = 0.0
        elif market_signal == "YELLOW":
            market_notes = ["YELLOW信号，半仓操作"]
            pos_pct = market_multiplier
        else:
            market_notes = ["GREEN信号，正常仓位"]
            pos_pct = market_multiplier

        # 3. 评分判断
        if score >= self.buy_threshold:
            base_action = "BUY"
            confidence = min(score / 10 * 10, 10)
            notes.append(f"评分{score:.1f}≥{self.buy_threshold}，进入买入范围")
        elif score >= self.watch_threshold:
            base_action = "WATCH"
            confidence = score / 10 * 7  # 置信度打折
            notes.append(f"评分{score:.1f}≥{self.watch_threshold}，仅观察")
        elif score >= self.reject_threshold:
            base_action = "SELL"
            confidence = (10 - score) / 10 * 5
            notes.append(f"评分{score:.1f}<{self.buy_threshold}，规避")
        else:
            base_action = "SELL"
            confidence = (10 - score) / 10 * 8
            veto_reasons.append(f"评分{score:.1f}≤{self.reject_threshold}一票否决")
            pos_pct = 0
            notes.append(f"评分{score:.1f}≤{self.reject_threshold}，一票否决")

        # 4. 合并 action（取更保守的）
        # market_action: 基于大盘信号的建议（None=大盘不限制操作）
        market_action = {"CLEAR": "CLEAR", "RED": "SELL"}.get(market_signal, None)
        action_priority = {"CLEAR": 0, "SELL": 1, "WATCH": 2, "HOLD": 3, "BUY": 4}
        if market_action is not None and action_priority.get(market_action, 0) < action_priority.get(base_action, 0):
            final_action = market_action
        else:
            final_action = base_action

        # 5. 仓位计算
        raw_pos_pct = pos_pct * (score / 10)  # 评分折扣
        final_pos_pct = min(raw_pos_pct, self.single_max_pct)  # 不超过单只上限

        # 实际可用仓位（考虑总仓位上限）
        current_exposure = self._get_current_exposure()  # TODO: 从 portfolio 读取
        remaining_pct = max(self.total_max_pct - current_exposure, 0)
        final_pos_pct = min(final_pos_pct, remaining_pct)

        position_amount = round(self.total_capital * final_pos_pct, 2)

        # 6. 止损止盈
        stop_loss_price = round(current_price * (1 - self.stop_loss_pct), 2)
        take_profit_price = round(current_price * (1 + self.t1_pct), 2)

        final_notes = market_notes + notes

        return Decision(
            action=final_action,
            confidence=round(confidence, 1),
            position_pct=round(final_pos_pct, 3),
            position_amount=position_amount,
            stop_loss_pct=self.stop_loss_pct,
            stop_loss_price=stop_loss_price,
            take_profit_pct=self.t1_pct,
            take_profit_price=take_profit_price,
            market_multiplier=market_multiplier,
            veto_reasons=veto_reasons,
            notes=final_notes,
        )

    def decide_batch(self, stock_scores: list) -> list:
        """
        批量决策

        Args:
            stock_scores: list of {
                "code": str,
                "name": str,
                "score": float,
                "price": float,
                "veto_signals": list,
            }

        Returns:
            list of Decision
        """
        market_signal = self.market_timer.get_signal()
        decisions = []
        for stock in stock_scores:
            d = self.decide(
                stock_code=stock["code"],
                score=stock["score"],
                current_price=stock["price"],
                market_signal=market_signal,
                veto_signals=stock.get("veto_signals", []),
            )
            d.stock_name = stock.get("name", stock["code"])
            decisions.append(d)
        return decisions

    # ---------------------------------------------------------------------------
    # 内部工具
    # ---------------------------------------------------------------------------

    def _get_current_exposure(self) -> float:
        """获取当前已用仓位占总资金比例（简化版）"""
        # TODO: 从 ObsidianVault 读取 portfolio.md 计算实时仓位
        return 0.0


# ---------------------------------------------------------------------------
# 便捷函数
# ---------------------------------------------------------------------------

def decide(stock_code: str, score: float, current_price: float,
           market_signal: Optional[str] = None) -> Decision:
    """便捷函数：对单只股票做决策"""
    decider = CompositeDecider()
    return decider.decide(stock_code, score, current_price, market_signal)


if __name__ == "__main__":
    decider = CompositeDecider()
    market_signal = get_market_signal()
    print(f"大盘信号: {market_signal}")
    print(f"仓位系数: {decider.market_timer.get_position_multiplier()}")
    print()

    # 模拟决策
    test_cases = [
        ("002487", "大金重工", 7.5, 10.5),
        ("002353", "杰瑞股份", 8.2, 45.0),
        ("300870", "欧陆通", 5.5, 68.0),
    ]

    for code, name, score, price in test_cases:
        d = decider.decide(code, score, price, market_signal)
        print(f"{name}({code}): 评分{score:.1f} → {d.action} | "
              f"仓位{d.position_pct:.1%} | "
              f"金额¥{d.position_amount:,.0f} | "
              f"止损{d.stop_loss_price} | "
              f"止盈{d.take_profit_price}")
        if d.notes:
            for n in d.notes:
                print(f"  • {n}")
