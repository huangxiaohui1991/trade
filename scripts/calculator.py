#!/usr/bin/env python3
"""
A股交易系统 v1.3 - 仓位/止损/止盈/加仓计算引擎
用法: python calculator.py <command> [args...]

命令:
  position    计算买入仓位和价格
  add         计算加仓相关数据
  stoploss    计算止损价
  takeprofit  计算止盈价
  risk_check  风控检查
"""

import sys
import json
from dataclasses import dataclass, asdict
from typing import Optional


# ============ 配置常量（v1.4 回测优化版） ============

TOTAL_CAPITAL = 450286          # 账户可交易现金（2026-04-08 更新，与 portfolio.md 一致）
STOCK_POSITION_RATIO = 0.60     # 股票仓位上限比例
SINGLE_STOCK_RATIO = 0.20       # 单只股票最大仓位比例
MAX_BUY_PER_WEEK = 2            # 每周最多买入次数
MAX_ADD_PER_STOCK_PER_WEEK = 1  # 单只每周最多加仓次数
MAX_HOLDING_STOCKS = 3          # 最大持仓股票数
MAX_LOSS_RATIO = 0.02           # 单笔最大亏损占总资金比例
FIRST_BUY_MIN = 20000           # 首次买入最小金额
FIRST_BUY_MAX = 30000           # 首次买入最大金额
ADD_BUY_MIN = 20000             # 加仓最小金额
ADD_BUY_MAX = 30000             # 加仓最大金额
HALF_BUY_MIN = 10000            # 大盘例外时减半买入最小
HALF_BUY_MAX = 15000            # 大盘例外时减半买入最大

# 止损参数
STOP_LOSS_PCT = 0.04            # 4% 动态止损（v1.4 从5%收紧）
ABSOLUTE_STOP_PCT = 0.07        # 7% 绝对止损

# 止盈参数
TAKE_PROFIT_1_PCT = 0.15        # 15% 卖1/3（从高点回撤5%触发第二批）
TAKE_PROFIT_2_DRAWDOWN = 0.05   # 从最高点回撤5% 卖1/3（累计卖2/3）
TAKE_PROFIT_3_DRAWDOWN = 0.08   # 从最高点回撤8% 或跌破20日线 清仓

# 加仓参数
ADD_TRIGGER_1_PCT = 0.07        # 第一次加仓触发涨幅（+7%）
ADD_TRIGGER_2_PCT = 0.12        # 第二次加仓触发涨幅（v1.4 从10%提高）

# 时间止损参数
TIME_STOP_DAYS = 15             # 时间止损天数（建仓后15个交易日未盈利则减仓）
TIME_STOP_AMPLITUDE = 0.04      # 时间止损振幅阈值（v1.4 从3%放宽）

# 追涨限制（v1.4）
MAX_ABOVE_MA20_PCT = 0.08       # 非趋势股：买入时离20日线最大距离8%
MAX_ABOVE_MA20_TREND = 0.15     # 趋势股（发散度>5%）：放宽到15%
MAX_DAY_GAIN = 0.05             # 当日涨幅超过5%不买
MAX_3DAY_GAIN = 0.10            # 近3日累计涨幅超过10%不买
BUY_CONFIRM_DAYS = 2            # 连续2天站上20日线才买入


@dataclass
class PositionResult:
    """买入仓位计算结果"""
    suggested_amount: float       # 建议买入金额
    suggested_shares: int         # 建议买入股数（100的整数倍）
    stop_loss_price: float        # 止损价
    absolute_stop_loss: float     # 绝对止损价（-7%）
    take_profit_1: float          # 第一批止盈价（+15%卖1/3）
    add_trigger_1: float          # 第一次加仓触发价（+5%）
    add_trigger_2: float          # 第二次加仓触发价（+10%）
    max_loss_amount: float        # 最大亏损金额
    remaining_stock_quota: float  # 剩余股票仓位额度
    remaining_single_quota: float # 该股剩余仓位额度
    can_buy: bool                 # 是否可以买入
    reject_reasons: list          # 不可买入的原因


@dataclass
class AddPositionResult:
    """加仓计算结果"""
    can_add: bool                 # 是否可以加仓
    suggested_amount: float       # 建议加仓金额
    suggested_shares: int         # 建议加仓股数
    new_avg_cost: float           # 加仓后新平均成本
    new_stop_loss: float          # 加仓后新止损价
    new_absolute_stop: float      # 加仓后新绝对止损价
    total_loss_if_stop: float     # 如果止损总亏损金额
    loss_ratio: float             # 亏损占总资金比例
    reject_reasons: list          # 不可加仓的原因
    priority_score: float         # 加仓优先级得分


def round_shares(amount: float, price: float) -> int:
    """计算买入股数（100股整数倍）"""
    shares = int(amount / price / 100) * 100
    return max(shares, 100)


def calc_stop_loss(avg_cost: float, ma20_price: Optional[float] = None) -> dict:
    """
    计算止损价（v1.3 三重保护）
    返回: {stop_loss: 动态止损价, absolute_stop: 绝对止损价}
    """
    cost_stop = avg_cost * (1 - STOP_LOSS_PCT)  # 4% 成本止损

    if ma20_price:
        ma_stop = ma20_price * 0.97  # 3% 均线止损
        dynamic_stop = max(cost_stop, ma_stop)
    else:
        dynamic_stop = cost_stop

    absolute_stop = avg_cost * (1 - ABSOLUTE_STOP_PCT)  # 7% 绝对止损线

    return {
        "stop_loss": round(dynamic_stop, 2),
        "absolute_stop": round(absolute_stop, 2),
        "cost_stop": round(cost_stop, 2),
        "ma_stop": round(ma20_price * 0.97, 2) if ma20_price else None
    }


def calc_take_profit(first_buy_price: float) -> dict:
    """
    计算止盈价
    返回三批止盈触发价
    """
    return {
        "batch_1_price": round(first_buy_price * (1 + TAKE_PROFIT_1_PCT), 2),  # +15% 卖1/3
        "batch_1_ratio": "1/3",
        "batch_2_trigger": "从最高点回撤5% 或 累计涨幅+30%",       # 卖1/3
        "batch_2_ratio": "1/3",
        "batch_3_trigger": "跌破20日线 或 从最高点回撤8%",         # 清仓
        "batch_3_ratio": "剩余1/3"
    }


def calc_position(
    buy_price: float,
    total_capital: float = TOTAL_CAPITAL,
    current_stock_value: float = 0,
    current_single_value: float = 0,
    ma20_price: Optional[float] = None,
    is_exception: bool = False,
    weekly_buy_count: int = 0
) -> dict:
    """
    计算首次买入仓位

    参数:
        buy_price: 计划买入价格
        total_capital: 账户总资金
        current_stock_value: 当前股票总市值
        current_single_value: 该股票当前持仓市值
        ma20_price: 20日均线价格（可选）
        is_exception: 是否大盘例外情况（减半买入）
        weekly_buy_count: 本周已买入次数
    """
    stock_limit = total_capital * STOCK_POSITION_RATIO
    single_limit = total_capital * SINGLE_STOCK_RATIO
    remaining_stock = stock_limit - current_stock_value
    remaining_single = single_limit - current_single_value

    reject_reasons = []

    # 检查买入次数
    if weekly_buy_count >= MAX_BUY_PER_WEEK:
        reject_reasons.append(f"本周已买入{weekly_buy_count}次，达到上限{MAX_BUY_PER_WEEK}次")

    # 确定买入金额范围
    if is_exception:
        buy_min, buy_max = HALF_BUY_MIN, HALF_BUY_MAX
    else:
        buy_min, buy_max = FIRST_BUY_MIN, FIRST_BUY_MAX

    # 取可用额度的最小值
    available = min(remaining_stock, remaining_single, buy_max)

    if available < buy_min:
        reject_reasons.append(f"可用额度{available:.0f}元不足最低买入{buy_min}元")

    suggested_amount = min(max(buy_min, available), buy_max)
    suggested_shares = round_shares(suggested_amount, buy_price)
    actual_amount = suggested_shares * buy_price

    # 止损计算
    stops = calc_stop_loss(buy_price, ma20_price)

    # 最大亏损检查
    max_loss = actual_amount * 0.07  # 绝对止损线亏损
    max_loss_ratio = max_loss / total_capital
    if max_loss_ratio > MAX_LOSS_RATIO:
        reject_reasons.append(f"最大亏损{max_loss:.0f}元({max_loss_ratio:.1%})超过总资金{MAX_LOSS_RATIO:.0%}限制")

    # 止盈计算
    tp = calc_take_profit(buy_price)

    result = {
        "suggested_amount": round(actual_amount, 2),
        "suggested_shares": suggested_shares,
        "buy_price": buy_price,
        "stop_loss": stops["stop_loss"],
        "absolute_stop_loss": stops["absolute_stop"],
        "take_profit_1_price": tp["batch_1_price"],
        "add_trigger_1": round(buy_price * (1 + ADD_TRIGGER_1_PCT), 2),
        "add_trigger_2": round(buy_price * (1 + ADD_TRIGGER_2_PCT), 2),
        "max_loss_amount": round(max_loss, 2),
        "max_loss_ratio": f"{max_loss_ratio:.2%}",
        "remaining_stock_quota": round(remaining_stock - actual_amount, 2),
        "remaining_single_quota": round(remaining_single - actual_amount, 2),
        "can_buy": len(reject_reasons) == 0,
        "reject_reasons": reject_reasons,
        "is_exception_mode": is_exception
    }
    return result


def calc_add_position(
    first_buy_price: float,
    current_avg_cost: float,
    current_shares: int,
    current_price: float,
    add_price: float,
    total_capital: float = TOTAL_CAPITAL,
    current_stock_value: float = 0,
    current_single_value: float = 0,
    ma20_price: Optional[float] = None,
    ma60_up: bool = True,
    weekly_buy_count: int = 0,
    weekly_add_count_this_stock: int = 0,
    highest_price: Optional[float] = None
) -> dict:
    """
    计算加仓

    参数:
        first_buy_price: 首次买入价
        current_avg_cost: 当前平均成本
        current_shares: 当前持有股数
        current_price: 当前价格
        add_price: 计划加仓价格
        ma20_price: 20日均线价格
        ma60_up: 60日均线是否向上
        weekly_buy_count: 本周已买入次数（含加仓）
        weekly_add_count_this_stock: 本周该股已加仓次数
        highest_price: 历史最高价（用于计算趋势强度）
    """
    reject_reasons = []

    # 计算涨幅
    gain_from_first = (current_price - first_buy_price) / first_buy_price

    # 判断加仓级别
    if gain_from_first >= ADD_TRIGGER_2_PCT:
        add_level = 2
        if not ma60_up:
            reject_reasons.append("第二次加仓要求60日线向上，当前不满足")
    elif gain_from_first >= ADD_TRIGGER_1_PCT:
        add_level = 1
        # 检查是否站上20日线
        if ma20_price and current_price < ma20_price:
            reject_reasons.append("加仓要求站上20日线，当前价格低于20日线")
    else:
        add_level = 0
        reject_reasons.append(f"涨幅{gain_from_first:.1%}未达到加仓条件（需≥{ADD_TRIGGER_1_PCT:.0%}）")

    # 检查买入次数
    if weekly_buy_count >= MAX_BUY_PER_WEEK:
        reject_reasons.append(f"本周已买入{weekly_buy_count}次，达到上限")

    # 检查单股加仓次数
    if weekly_add_count_this_stock >= MAX_ADD_PER_STOCK_PER_WEEK:
        reject_reasons.append(f"本周该股已加仓{weekly_add_count_this_stock}次，达到上限")

    # 计算加仓金额
    single_limit = total_capital * SINGLE_STOCK_RATIO
    remaining_single = single_limit - current_single_value
    suggested_amount = min(ADD_BUY_MAX, remaining_single)

    if suggested_amount < ADD_BUY_MIN:
        reject_reasons.append(f"该股剩余额度{remaining_single:.0f}元不足最低加仓{ADD_BUY_MIN}元")
        suggested_amount = 0

    add_shares = round_shares(suggested_amount, add_price) if suggested_amount > 0 else 0
    actual_add_amount = add_shares * add_price

    # 计算新平均成本
    total_cost = current_avg_cost * current_shares + actual_add_amount
    new_total_shares = current_shares + add_shares
    new_avg_cost = total_cost / new_total_shares if new_total_shares > 0 else 0

    # 计算新止损价
    new_stops = calc_stop_loss(new_avg_cost, ma20_price)

    # v1.3: 加仓风险检查 — 止损后总亏损必须 < 总资金 2%
    total_value_after_add = new_total_shares * add_price
    total_loss_if_stop = total_value_after_add - new_total_shares * new_stops["absolute_stop"]
    loss_ratio = total_loss_if_stop / total_capital

    if loss_ratio > MAX_LOSS_RATIO:
        reject_reasons.append(
            f"加仓后如触发止损，总亏损{total_loss_if_stop:.0f}元({loss_ratio:.1%})超过总资金{MAX_LOSS_RATIO:.0%}限制"
        )

    # v1.3: 加仓优先级得分（涨幅/回撤比）
    if highest_price and highest_price > first_buy_price:
        drawdown = (highest_price - current_price) / highest_price if highest_price > current_price else 0.001
        priority_score = gain_from_first / max(drawdown, 0.001)
    else:
        priority_score = gain_from_first / 0.001 if gain_from_first > 0 else 0

    result = {
        "can_add": len(reject_reasons) == 0,
        "add_level": add_level,
        "gain_from_first": f"{gain_from_first:.1%}",
        "suggested_amount": round(actual_add_amount, 2),
        "suggested_shares": add_shares,
        "new_avg_cost": round(new_avg_cost, 2),
        "new_stop_loss": new_stops["stop_loss"],
        "new_absolute_stop": new_stops["absolute_stop"],
        "total_loss_if_stop": round(total_loss_if_stop, 2),
        "loss_ratio": f"{loss_ratio:.2%}",
        "priority_score": round(priority_score, 2),
        "reject_reasons": reject_reasons
    }
    return result


def calc_time_stop(buy_price: float, prices_5d: list) -> dict:
    """
    v1.3 时间止损检查
    prices_5d: 买入后5个交易日的[最高价, 最低价]列表
    """
    if len(prices_5d) < 5:
        return {"should_stop": False, "reason": f"仅{len(prices_5d)}个交易日，未满5天"}

    all_highs = [p[0] for p in prices_5d]
    all_lows = [p[1] for p in prices_5d]
    period_high = max(all_highs)
    period_low = min(all_lows)
    amplitude = (period_high - period_low) / buy_price

    # v1.4: 振幅 < 4% 才触发
    should_stop = amplitude < TIME_STOP_AMPLITUDE

    return {
        "should_stop": should_stop,
        "amplitude": f"{amplitude:.2%}",
        "period_high": period_high,
        "period_low": period_low,
        "threshold": f"{TIME_STOP_AMPLITUDE:.0%}",
        "reason": f"5日振幅{amplitude:.2%}{'<' + str(int(TIME_STOP_AMPLITUDE*100)) + '%，建议卖出' if should_stop else '≥' + str(int(TIME_STOP_AMPLITUDE*100)) + '%，继续持有'}"
    }


def calc_market_stop(
    market_below_ma20: bool,
    market_below_ma60_days: int,
    monthly_market_stop_count: int
) -> dict:
    """
    v1.3 大盘止损检查

    参数:
        market_below_ma20: 大盘是否跌破20日线
        market_below_ma60_days: 大盘连续在60日线下方的天数
        monthly_market_stop_count: 本月因大盘止损减仓的次数
    """
    actions = []
    level = "正常"

    if monthly_market_stop_count >= 2:
        # v1.3: 熊市空仓规则
        level = "空仓观望"
        actions.append("本月已因大盘止损减仓2次以上，进入空仓观望模式")
        actions.append("等大盘站上60日线后再恢复交易")
    elif market_below_ma60_days >= 3:
        # v1.3: 连续3日确认
        level = "清仓"
        actions.append(f"大盘连续{market_below_ma60_days}日收盘在60日线下方，清仓所有股票")
    elif market_below_ma20:
        level = "减仓"
        actions.append("大盘跌破20日线，减仓至50%以下")

    return {
        "level": level,
        "market_below_ma20": market_below_ma20,
        "market_below_ma60_days": market_below_ma60_days,
        "monthly_market_stop_count": monthly_market_stop_count,
        "actions": actions
    }


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1]

    if command == "position":
        # 用法: python calculator.py position <buy_price> [total_capital] [current_stock_value] [current_single_value] [ma20_price] [is_exception] [weekly_buy_count]
        args = sys.argv[2:]
        if len(args) < 1:
            print("用法: python calculator.py position <buy_price> [total_capital] [current_stock_value] [current_single_value] [ma20_price] [is_exception] [weekly_buy_count]")
            sys.exit(1)
        result = calc_position(
            buy_price=float(args[0]),
            total_capital=float(args[1]) if len(args) > 1 else TOTAL_CAPITAL,
            current_stock_value=float(args[2]) if len(args) > 2 else 0,
            current_single_value=float(args[3]) if len(args) > 3 else 0,
            ma20_price=float(args[4]) if len(args) > 4 and args[4] != "0" else None,
            is_exception=args[5].lower() == "true" if len(args) > 5 else False,
            weekly_buy_count=int(args[6]) if len(args) > 6 else 0
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))

    elif command == "add":
        # 用法: python calculator.py add <first_buy_price> <avg_cost> <shares> <current_price> <add_price> [更多参数...]
        args = sys.argv[2:]
        if len(args) < 5:
            print("用法: python calculator.py add <first_buy_price> <avg_cost> <shares> <current_price> <add_price>")
            sys.exit(1)
        result = calc_add_position(
            first_buy_price=float(args[0]),
            current_avg_cost=float(args[1]),
            current_shares=int(args[2]),
            current_price=float(args[3]),
            add_price=float(args[4]),
            total_capital=float(args[5]) if len(args) > 5 else TOTAL_CAPITAL,
            current_stock_value=float(args[6]) if len(args) > 6 else 0,
            current_single_value=float(args[7]) if len(args) > 7 else 0,
            ma20_price=float(args[8]) if len(args) > 8 and args[8] != "0" else None,
            ma60_up=args[9].lower() == "true" if len(args) > 9 else True,
            weekly_buy_count=int(args[10]) if len(args) > 10 else 0,
            weekly_add_count_this_stock=int(args[11]) if len(args) > 11 else 0,
            highest_price=float(args[12]) if len(args) > 12 and args[12] != "0" else None
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))

    elif command == "stoploss":
        # 用法: python calculator.py stoploss <avg_cost> [ma20_price]
        args = sys.argv[2:]
        if len(args) < 1:
            print("用法: python calculator.py stoploss <avg_cost> [ma20_price]")
            sys.exit(1)
        result = calc_stop_loss(
            avg_cost=float(args[0]),
            ma20_price=float(args[1]) if len(args) > 1 and args[1] != "0" else None
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))

    elif command == "takeprofit":
        # 用法: python calculator.py takeprofit <first_buy_price>
        args = sys.argv[2:]
        if len(args) < 1:
            print("用法: python calculator.py takeprofit <first_buy_price>")
            sys.exit(1)
        result = calc_take_profit(float(args[0]))
        print(json.dumps(result, ensure_ascii=False, indent=2))

    elif command == "timestop":
        # 用法: python calculator.py timestop <buy_price> <h1,l1> <h2,l2> ...
        args = sys.argv[2:]
        if len(args) < 2:
            print("用法: python calculator.py timestop <buy_price> <high1,low1> <high2,low2> ...")
            sys.exit(1)
        buy_price = float(args[0])
        prices = []
        for p in args[1:]:
            h, l = p.split(",")
            prices.append([float(h), float(l)])
        result = calc_time_stop(buy_price, prices)
        print(json.dumps(result, ensure_ascii=False, indent=2))

    elif command == "marketstop":
        # 用法: python calculator.py marketstop <below_ma20:true/false> <below_ma60_days> <monthly_stop_count>
        args = sys.argv[2:]
        if len(args) < 3:
            print("用法: python calculator.py marketstop <below_ma20> <below_ma60_days> <monthly_stop_count>")
            sys.exit(1)
        result = calc_market_stop(
            market_below_ma20=args[0].lower() == "true",
            market_below_ma60_days=int(args[1]),
            monthly_market_stop_count=int(args[2])
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))

    else:
        print(f"未知命令: {command}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
